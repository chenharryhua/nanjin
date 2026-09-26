package com.github.chenharryhua.nanjin.guard.batch

import cats.Applicative
import cats.data.{Kleisli, StateT}
import cats.effect.Temporal
import cats.effect.kernel.Async
import cats.syntax.applicative.given
import cats.syntax.applicativeError.given
import cats.syntax.either.catsSyntaxEither
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import cats.syntax.monadError.catsSyntaxMonadErrorRethrow
import cats.syntax.traverse.given
import com.github.chenharryhua.nanjin.guard.metrics.MetricScope

import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.ScalaDurationOps

/** Low-overhead batch construction API for short-lived jobs.
  *
  * Obtain a `BatchLight` from `Agent.batchLight(label)`. It has the same sequential, parallel, and monadic
  * shapes as `Batch`, but returns `F` values directly and omits the metrics-backed progress machinery. Use
  * `quasiBatch` to retain per-job failures or `valueBatch` to raise them.
  */
object BatchLight:

  /*
   * Runners
   */

  /** Common runner operations for the sequential and parallel light batches.
    *
    * Like the full `Batch` runners the two shapes differ only in how they traverse the job list, so that one
    * choice is the abstract `traverseJobs`. Unlike `Batch` there is no metrics panel or logging, so the
    * shared body is just: mint a batch id, time the traversal, and assemble the result.
    */
  sealed abstract protected class BatchRunner[F[_], A](using F: Async[F]) {

    protected def mode: BatchMode

    protected def scope: MetricScope

    protected def jobs: List[JobNameIndex[F, A]]

    protected def batchIdGenerator: F[BatchId]

    protected def executor: JobExecutor[F, A]

    /** Run `f` over every job, in this runner's traversal order (parallel vs sequential). */
    protected def traverseJobs[B](f: JobNameIndex[F, A] => F[B]): F[List[B]]

    /** Reject successful values that do not satisfy `f`. */
    def withPostCondition(f: A => Boolean): BatchRunner[F, A]

    /** Execute while preserving per-job success or failure state. */
    final def quasiBatch: F[QuasiBatch[A]] =
      batchIdGenerator.flatMap { batchId =>
        F.timed(traverseJobs(executor.quasiJob(_, batchId).compute)).map {
          case (fd: FiniteDuration, js: List[JobState[A]]) =>
            QuasiBatch(scope = scope, spent = fd.toJava, mode = mode, batchId = batchId, outcomes = js)
        }
      }

    /** Execute and raise on failure, returning successful values. */
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
            outcomes = jv.map(v => JobState(v.record, Right(v.result))),
            result = jv.map(_.result))
        }
      }
  }

  /*
   * Parallel
   */
  final class Parallel[F[_], A] private[BatchLight] (
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
  final class Sequential[F[_], A] private[BatchLight] (
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

  final class JobBuilder[F[_]] private[BatchLight] (val scope: MetricScope, batchIdGenerator: F[BatchId])(
    using F: Async[F]):

    final class Monadic[A] private[BatchLight] (
      private val kleisli: Kleisli[StateT[F, JobCursor, *], BatchId, ExecutionState[A]]):

      /** Sequence a dependent monadic job when the previous job succeeds. */
      def flatMap[B](f: A => Monadic[B]): Monadic[B] = {
        val runB: Kleisli[StateT[F, JobCursor, *], BatchId, ExecutionState[B]] =
          Kleisli { (batchId: BatchId) =>
            StateT { (cursor: JobCursor) =>
              kleisli(batchId).run(cursor).flatMap {
                case (nextCursor: JobCursor, execState: ExecutionState[A]) =>
                  execState.eoa match {
                    case Left(ex) => (nextCursor -> execState.update[B](ex)).pure[F]
                    case Right(a) =>
                      f(a).kleisli(batchId).run(nextCursor).map {
                        case (finalCursor: JobCursor, nextState: ExecutionState[B]) =>
                          finalCursor -> execState.prependHistory[B](nextState)
                      }
                  }
              }
            }
          }
        new Monadic[B](runB)
      }

      /** Transform a successful monadic job value without adding a job. */
      def map[B](f: A => B): Monadic[B] = new Monadic[B](kleisli.map(_.map(f)))

      /** Filter a successful monadic value; a rejected value fails the step and stops the chain. */
      def withFilter(f: A => Boolean): Monadic[A] =
        new Monadic[A](
          Kleisli { (batchId: BatchId) =>
            kleisli(batchId).map { case unchange @ ExecutionState(eoa, history) =>
              eoa match {
                case Left(_)      => unchange
                case Right(value) =>
                  if (f(value))
                    unchange
                  else {
                    val err = PostConditionUnsatisfied(history.headOption.map(_.record.job))
                    ExecutionState[A](Left(err), history)
                  }
              }
            }
          }
        )

      /** Execute the monadic batch and return its result in `F`.
        *
        * Job outcomes never fail this effect: a job that throws, a lifted `untracked`/`pure` step that
        * throws, and a `withFilter` rejection are all captured and surface as `Left` in the returned
        * `MonadicBatch.result`, short-circuiting the chain. Read `result` to observe success or failure of
        * the work. Unlike `Batch`, `BatchLight` does no lifecycle logging and keeps no metrics panel or
        * resource scope, so there is no batch machinery around the jobs that could fail this effect.
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
          outcomes = history.reverse,
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

    /** Add a pure value to the monadic batch without creating a job. */
    def pure[A](a: A): Monadic[A] =
      new Monadic[A](Kleisli { _ =>
        StateT(cursor => (cursor -> ExecutionState(Right(a), Nil)).pure[F])
      })

    /** Add an effectful value to the monadic batch without creating a job.
      *
      * The effect is not tracked, timed, or reported. If it fails, the failure short-circuits the chain: no
      * further jobs run and the failure surfaces as `Left` in the batch `result`. The original exception is
      * wrapped in `UntrackedStepException` so it is distinguishable there from a tracked job's failure.
      */
    def untracked[A](fa: F[A]): Monadic[A] =
      new Monadic[A](Kleisli { _ =>
        StateT(cursor =>
          fa.attempt.map(a => cursor -> ExecutionState(a.leftMap(UntrackedStepException(_)), Nil)))
      })

    /** Shared constructor for effect-backed jobs. The job runs under `attempt`: a thrown exception is
      * recorded as an unsuccessful job and propagated as the monadic result, stopping the chain; a successful
      * effect is judged by `predicate` to set the job's `succeeded` flag, but its value flows on regardless
      * so the chain continues.
      */
    private def create[A](name: String, fa: F[A], predicate: A => Boolean): Monadic[A] =
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
              end <- Temporal[F].monotonic
            } yield {
              val succeeded = eoa.fold(_ => false, predicate)
              val completed = JobState(JobRecord(job, start, end, succeeded), eoa.as(()))
              JobCursor(index + 1, end) -> ExecutionState(eoa = eoa, history = List(completed))
            }
          }
        }
      )

    /** Add a named effect-backed job. The job succeeds unless its effect throws, in which case the exception
      * stops the chain.
      *
      * @param name
      *   name of the job
      * @param fa
      *   the effect to run
      */
    def apply[A](name: String, fa: F[A]): Monadic[A] = create[A](name, fa, _ => true)

    /** Add a named effect-backed job whose success is decided by `predicate`.
      *
      * A rejected value (`predicate` returns false) marks the job as failed in its `JobRecord` but does not
      * stop the chain: the value still flows to later jobs. To reject a value and stop the chain instead, use
      * `withFilter`. A thrown exception is always recorded as failed and stops the chain, regardless of
      * `predicate`.
      *
      * @param name
      *   name of the job
      * @param fa
      *   the effect to run
      * @param predicate
      *   applied to a successful value to decide whether the job counts as succeeded
      */
    def apply[A](name: String, fa: F[A], predicate: A => Boolean): Monadic[A] =
      create[A](name, fa, predicate)

  end JobBuilder
end BatchLight

/** Lightweight batch façade for short-lived jobs.
  *
  * It intentionally avoids the richer lifecycle and progress-tracking machinery of Batch and focuses on
  * straightforward, low-overhead execution.
  */
final class BatchLight[F[_]: Async] private[guard] (scope: MetricScope, batchIdGenerator: F[BatchId]):

  /** Create a sequential batch from named effects. */
  def sequential[A](fas: (String, F[A])*): BatchLight.Sequential[F, A] = {
    val jobs = fas.toList.zipWithIndex.map { case ((name, fa), idx) =>
      JobNameIndex[F, A](name, idx + 1, fa)
    }
    new BatchLight.Sequential[F, A](
      _ => true,
      scope,
      jobs,
      batchIdGenerator)
  }

  /** Create a parallel batch with an explicit positive parallelism. */
  def parallel[A](parallelism: Int)(fas: (String, F[A])*): BatchLight.Parallel[F, A] = {
    require(parallelism > 0, s"parallelism must be > 0, but was $parallelism")
    val jobs = fas.toList.zipWithIndex.map { case ((name, fa), idx) =>
      JobNameIndex[F, A](name, idx + 1, fa)
    }
    new BatchLight.Parallel[F, A](
      _ => true,
      scope,
      parallelism,
      jobs,
      batchIdGenerator)
  }

  def parallel[A](fas: (String, F[A])*): BatchLight.Parallel[F, A] =
    parallel[A](math.max(1, fas.size))(fas*)

  def monadic[A](f: BatchLight.JobBuilder[F] => A): A = {
    val builder = new BatchLight.JobBuilder[F](scope, batchIdGenerator)
    f(builder)
  }
end BatchLight
