package com.github.chenharryhua.nanjin.guard.batch

import cats.Applicative
import cats.data.{Kleisli, NonEmptyList, StateT}
import cats.effect.kernel.Async
import cats.syntax.applicative.given
import cats.syntax.applicativeError.given
import cats.syntax.either.given
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import cats.syntax.monadError.given
import cats.syntax.traverse.given
import com.github.chenharryhua.nanjin.guard.metrics.MetricScope
import io.circe.{Encoder, Json}

import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.ScalaDurationOps

/** Constructors for lightweight, untraced batch execution. */
object Batch:

  /*
   * Runners
   */

  /** Common runner operations for the sequential and parallel light batches.
    *
    * Like the `BatchMetered` runners the two shapes differ only in how they traverse the job list, so that
    * one choice is the abstract `traverseJobs`. Unlike `BatchMetered` there is no metrics panel or logging,
    * so the shared body is just: mint a batch id, time the traversal, and assemble the result.
    */
  sealed abstract protected class BatchRunner[F[_], A](using F: Async[F]) {

    protected def mode: BatchMode

    protected def scope: MetricScope

    protected def jobs: List[JobNameIndex[F, A]]

    protected def batchIdGenerator: F[BatchId]

    protected def executor: JobExecutor[F, A]

    /** Run `f` over every job, in this runner's traversal order (parallel vs sequential). */
    protected def traverseJobs[B](f: JobNameIndex[F, A] => F[B]): F[List[B]]

    /** Set the post-condition applied to each successful job. Quasi-batches retain rejected values as unmet;
      * value-batches fail with `PostConditionUnsatisfied`.
      */
    def withPostCondition(f: A => Boolean): BatchRunner[F, A]

    /** Run every job and retain its result or failure in the returned outcomes. */
    final def quasiBatch: F[QuasiBatch[A]] =
      batchIdGenerator.flatMap { batchId =>
        F.timed(traverseJobs(executor.quasiJob(_, batchId).compute)).map {
          case (fd: FiniteDuration, js: List[JobState[A]]) =>
            QuasiBatch(scope = scope, spent = fd.toJava, mode = mode, batchId = batchId, outcomes = js)
        }
      }

    /** Run every job and return its values, raising on the first job failure or rejected value. */
    final def valueBatch: F[ValueBatch[A]] =
      batchIdGenerator.flatMap { batchId =>
        F.timed(traverseJobs { jni =>
          executor.valueJob(jni, batchId)
            .compute
            .map(js => js.result.map(JobValue(js.record, _)))
            .rethrow
        }).map { case (fd: FiniteDuration, jv: List[JobValue[A]]) =>
          ValueBatch(
            scope = scope,
            spent = fd.toJava,
            mode = mode,
            batchId = batchId,
            outcomes = jv.map(v => JobState(v.record, JobFlag.Accepted, Right(v.result))),
            result = jv.map(_.result)
          )
        }
      }
  }

  /*
   * Parallel
   */
  final class Parallel[F[_], A] private[Batch] (
    predicate: A => Boolean,
    protected val scope: MetricScope,
    parallelism: Int,
    protected val jobs: List[JobNameIndex[F, A]],
    protected val batchIdGenerator: F[BatchId])(using F: Async[F])
      extends BatchRunner[F, A] {

    override protected val mode: BatchMode = BatchMode.Parallel(parallelism)
    override protected val executor: JobExecutor[F, A] =
      JobExecutor[F, A](predicate = predicate, mode = mode, scope = scope, log = None)

    override protected def traverseJobs[B](f: JobNameIndex[F, A] => F[B]): F[List[B]] =
      F.parTraverseN(parallelism)(jobs)(f)

    override def withPostCondition(f: A => Boolean): Parallel[F, A] =
      new Parallel[F, A](predicate = f, scope, parallelism, jobs, batchIdGenerator)
  }

  /*
   * Sequential
   */
  final class Sequential[F[_], A] private[Batch] (
    predicate: A => Boolean,
    protected val scope: MetricScope,
    protected val jobs: List[JobNameIndex[F, A]],
    protected val batchIdGenerator: F[BatchId])(using F: Async[F])
      extends BatchRunner[F, A] {

    override protected val mode: BatchMode = BatchMode.Sequential
    override protected val executor: JobExecutor[F, A] =
      JobExecutor[F, A](predicate = predicate, mode = mode, scope = scope, log = None)

    override protected def traverseJobs[B](f: JobNameIndex[F, A] => F[B]): F[List[B]] =
      jobs.traverse(f)

    override def withPostCondition(f: A => Boolean): Sequential[F, A] =
      new Sequential[F, A](predicate = f, scope, jobs, batchIdGenerator)
  }

  /*
   * Monadic
   */

  /** Builder for monadic batches whose jobs are composed with `map` and `flatMap`. */
  final class JobBuilder[F[_]] private[Batch] (scope: MetricScope, batchIdGenerator: F[BatchId])(using
    F: Async[F]):

    final class Monadic[A] private[Batch] (
      private val kleisli: Kleisli[StateT[F, JobCursor, *], BatchId, ExecutionState[A]]):

      /** Run the next segment only if this one succeeds. */
      def flatMap[B](f: A => Monadic[B]): Monadic[B] =
        new Monadic[B](MonadicOps.flatMap(kleisli, a => f(a).kleisli))

      /** Transform a successful value without adding a job. */
      def map[B](f: A => B): Monadic[B] = new Monadic[B](MonadicOps.map(kleisli, f))

      /** Reject a value that does not satisfy `f`, failing the chain with `PostConditionUnsatisfied`. */
      def withFilter(f: A => Boolean): Monadic[A] =
        new Monadic[A](MonadicOps.withFilter(kleisli, f))

      /** Capture a chain failure as a value so later segments can run; inspect or rethrow the captured error.
        */
      def attempt: Monadic[Either[Throwable, A]] =
        new Monadic[Either[Throwable, A]](MonadicOps.attempt(kleisli))

      /** Mark the current tracked job accepted or unmet without stopping the chain. */
      def predicate(f: A => Boolean): Monadic[A] =
        new Monadic[A](MonadicOps.predicate(kleisli, f))

      /** Attach a JSON representation of the current tracked job's result without changing the chain value.
        */
      def renderOutcome(f: A => Json): Monadic[A] =
        new Monadic[A](MonadicOps.renderOutcome(kleisli, f))

      /** Encode the current tracked job's result as JSON without changing the chain value. */
      def render(using ev: Encoder[A]): Monadic[A] =
        renderOutcome(ev.apply)

      /** Run the chain and return its final `Either` and recorded job outcomes. Job and filter failures are
        * captured in the result; they do not fail the surrounding effect.
        */
      def monadicBatch: F[MonadicBatch[A]] =
        for {
          batchId <- batchIdGenerator
          start <- F.monotonic
          (_, ExecutionState(eoa, history)) <- kleisli(batchId).run(JobCursor(1, start))
          end <- F.monotonic
        } yield MonadicBatch(
          scope = scope,
          spent = (end - start).toJava,
          batchId = batchId,
          outcomes = history.toList.flatten.reverse,
          result = eoa)
    end Monadic
    object Monadic:
      given Applicative[Monadic] with
        override def pure[A](a: A): Monadic[A] = JobBuilder.this.pure(a)
        override def ap[A, B](ff: Monadic[A => B])(fa: Monadic[A]): Monadic[B] =
          ff.flatMap(fa.map)
      end given
    end Monadic

    // job constructors

    /** Add a value to the chain without recording a job. */
    def pure[A](a: A): Monadic[A] =
      new Monadic[A](Kleisli { _ =>
        StateT(cursor => (cursor -> ExecutionState(Right(a), NonEmptyList.one(None))).pure[F])
      })

    /** Add an untracked effect to the chain. Its failure stops the chain and is captured in the batch result.
      */
    def untracked[A](fa: F[A]): Monadic[A] =
      new Monadic[A](Kleisli { _ =>
        StateT(cursor =>
          fa.attempt.map(a =>
            cursor -> ExecutionState(a.leftMap(UntrackedStepException(_)), NonEmptyList.one(None))))
      })

    /** Add a named job. A failure is captured in the chain result and prevents later segments from running.
      */
    def apply[A](name: String, fa: F[A]): Monadic[A] =
      new Monadic[A](
        Kleisli { (batchId: BatchId) =>
          StateT { case JobCursor(index: Int, start: FiniteDuration) =>
            val job: Job =
              Job(
                name = name,
                index = index,
                scope = scope,
                mode = BatchMode.Monadic,
                kind = None,
                batchId = batchId)

            for {
              eoa <- fa.attempt
              end <- F.monotonic
            } yield {
              val flag = if (eoa.isRight) JobFlag.Accepted else JobFlag.Failed
              val completed = JobState(JobRecord(job, start, end, None), flag, eoa.as(Json.Null))
              JobCursor(index + 1, end) ->
                ExecutionState(eoa = eoa, history = NonEmptyList.one(Some(completed)))
            }
          }
        }
      )
  end JobBuilder
end Batch

/** Lightweight batch façade for named effects, obtained from `Agent.batch`.
  *
  * Choose sequential or parallel execution for independent jobs, or monadic execution for dependent jobs.
  * Results are returned directly in `F`; use `BatchMetered` for lifecycle logs and progress metrics, or
  * `BatchTraced` to create job spans.
  */
final class Batch[F[_]: Async] private[guard] (scope: MetricScope, batchIdGenerator: F[BatchId]):

  /** Create a batch that runs the named effects in order. */
  def sequential[A](fas: (String, F[A])*): Batch.Sequential[F, A] = {
    val jobs = fas.toList.zipWithIndex.map { case ((name, fa), idx) =>
      JobNameIndex[F, A](name, idx + 1, fa)
    }
    new Batch.Sequential[F, A](_ => true, scope, jobs, batchIdGenerator)
  }

  /** Create a batch that runs named effects concurrently up to `parallelism`.
    *
    * Requires `parallelism > 0`; otherwise this method throws `IllegalArgumentException`.
    */
  def parallel[A](parallelism: Int)(fas: (String, F[A])*): Batch.Parallel[F, A] = {
    require(parallelism > 0, s"parallelism must be > 0, but was $parallelism")
    val jobs = fas.toList.zipWithIndex.map { case ((name, fa), idx) =>
      JobNameIndex[F, A](name, idx + 1, fa)
    }
    new Batch.Parallel[F, A](_ => true, scope, parallelism, jobs, batchIdGenerator)
  }

  /** Create a parallel batch with concurrency set to the number of jobs, or one when the list is empty. */
  def parallel[A](fas: (String, F[A])*): Batch.Parallel[F, A] =
    parallel[A](math.max(1, fas.size))(fas*)

  /** Build a chain of jobs whose later steps may depend on earlier results. */
  def monadic[A](f: Batch.JobBuilder[F] => A): A = {
    val builder = new Batch.JobBuilder[F](scope, batchIdGenerator)
    f(builder)
  }
end Batch
