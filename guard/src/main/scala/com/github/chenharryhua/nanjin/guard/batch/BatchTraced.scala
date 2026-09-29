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
import org.typelevel.otel4s.trace.{Span, SpanOps, Tracer}

import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.ScalaDurationOps

final private[guard] case class BatchTracer[F[_]](tracer: Tracer[F], parent: SpanOps[F])

object BatchTraced:

  /*
   * Runners
   */

  sealed abstract protected class BatchRunner[F[_], A](using F: Async[F]) {

    protected def mode: BatchMode

    protected def scope: MetricScope

    protected def jobs: List[JobNameIndex[F, A]]

    protected def batchIdGenerator: F[BatchId]

    protected def executor: JobExecutor[F, A]

    protected def traverseJobs[B](f: JobNameIndex[F, A] => F[B]): F[List[B]]

    protected def batchTracer: BatchTracer[F]

    def withPostCondition(f: A => Boolean): BatchRunner[F, A]

    final def quasiBatch: F[QuasiBatch[A]] =
      batchIdGenerator.flatMap { batchId =>
        batchTracer.parent.surround {
          F.timed(traverseJobs(executor.quasiJob(_, batchId).compute)).map {
            case (fd: FiniteDuration, js: List[JobState[A]]) =>
              QuasiBatch(scope = scope, spent = fd.toJava, mode = mode, batchId = batchId, outcomes = js)
          }
        }
      }

    final def valueBatch: F[ValueBatch[A]] =
      batchIdGenerator.flatMap { batchId =>
        batchTracer.parent.surround {
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
  }

  /*
   * Parallel
   */
  final class Parallel[F[_], A] private[BatchTraced] (
    predicate: A => Boolean,
    protected val scope: MetricScope,
    parallelism: Int,
    protected val jobs: List[JobNameIndex[F, A]],
    protected val batchIdGenerator: F[BatchId],
    protected val batchTracer: BatchTracer[F]
  )(using F: Async[F])
      extends BatchRunner[F, A] {

    override protected val mode: BatchMode = BatchMode.Parallel(parallelism)
    override protected val executor: JobExecutor[F, A] =
      JobExecutor[F, A](predicate = predicate, mode = mode, scope = scope, log = None)

    override protected def traverseJobs[B](f: JobNameIndex[F, A] => F[B]): F[List[B]] =
      F.parTraverseN(parallelism)(jobs)(f)

    override def withPostCondition(f: A => Boolean): Parallel[F, A] =
      new Parallel[F, A](predicate = f, scope, parallelism, jobs, batchIdGenerator, batchTracer)
  }

  /*
   * Sequential
   */
  final class Sequential[F[_], A] private[BatchTraced] (
    predicate: A => Boolean,
    protected val scope: MetricScope,
    protected val jobs: List[JobNameIndex[F, A]],
    protected val batchIdGenerator: F[BatchId],
    protected val batchTracer: BatchTracer[F])(using F: Async[F])
      extends BatchRunner[F, A] {

    override protected val mode: BatchMode = BatchMode.Sequential
    override protected val executor: JobExecutor[F, A] =
      JobExecutor[F, A](predicate = predicate, mode = mode, scope = scope, log = None)

    override protected def traverseJobs[B](f: JobNameIndex[F, A] => F[B]): F[List[B]] =
      jobs.traverse(f)

    override def withPostCondition(f: A => Boolean): Sequential[F, A] =
      new Sequential[F, A](predicate = f, scope, jobs, batchIdGenerator, batchTracer)
  }

  /*
   * Monadic
   */

  final class JobBuilder[F[_]] private[BatchTraced] (
    val scope: MetricScope,
    batchIdGenerator: F[BatchId],
    batchTracer: BatchTracer[F])(using F: Async[F]):

    final class Monadic[A] private[BatchTraced] (
      private val kleisli: Kleisli[StateT[F, JobCursor, *], BatchId, ExecutionState[A]]):

      def flatMap[B](f: A => Monadic[B]): Monadic[B] =
        new Monadic[B](MonadicOps.flatMap(kleisli, a => f(a).kleisli))

      def map[B](f: A => B): Monadic[B] = new Monadic[B](MonadicOps.map(kleisli, f))

      def withFilter(f: A => Boolean): Monadic[A] =
        new Monadic[A](MonadicOps.withFilter(kleisli, f))

      /** Capture a job-chain failure as an inner `Left` and continue the chain.
        *
        * The returned batch result is `Right(Left(error))` when the chain has failed. The error is therefore
        * surfaced as data and must be inspected or rethrown by the caller.
        */
      def attempt: Monadic[Either[Throwable, A]] =
        new Monadic[Either[Throwable, A]](MonadicOps.attempt(kleisli))

      /** Record whether the current tracked step satisfies `f` without short-circuiting the chain.
        *
        * If the current step is untracked, there is no job outcome to update and this has no effect.
        */
      def predicate(f: A => Boolean): Monadic[A] =
        new Monadic[A](MonadicOps.predicate(kleisli, f))

      /** Attach a JSON representation of the current tracked step's value to its outcome without changing the
        * value passed along the chain. An untracked step has no outcome to update, so this has no effect.
        */
      def renderOutcome(f: A => Json): Monadic[A] =
        new Monadic[A](MonadicOps.renderOutcome(kleisli, f))

      /** Encode the current tracked step's value as JSON for its outcome without changing the chain value. */
      def render(using ev: Encoder[A]): Monadic[A] =
        renderOutcome(ev.apply)

      def monadicBatch: F[MonadicBatch[A]] =
        for {
          batchId <- batchIdGenerator
          result <- batchTracer.parent.surround {
            for {
              start <- F.monotonic
              (_, ExecutionState(eoa, history)) <- kleisli(batchId).run(JobCursor(1, start))
              end <- F.monotonic
            } yield MonadicBatch(
              scope = scope,
              spent = (end - start).toJava,
              batchId = batchId,
              outcomes = history.toList.flatten.reverse,
              result = eoa)
          }
        } yield result
    end Monadic
    object Monadic:
      given Applicative[Monadic] with
        override def pure[A](a: A): Monadic[A] = JobBuilder.this.pure(a)
        override def ap[A, B](ff: Monadic[A => B])(fa: Monadic[A]): Monadic[B] =
          ff.flatMap(fa.map)
      end given
    end Monadic

    def pure[A](a: A): Monadic[A] =
      new Monadic[A](Kleisli { _ =>
        StateT(cursor => (cursor -> ExecutionState(Right(a), NonEmptyList.one(None))).pure[F])
      })
    def untracked[A](fa: F[A]): Monadic[A] =
      new Monadic[A](Kleisli { _ =>
        StateT(cursor =>
          fa.attempt.map(a =>
            cursor -> ExecutionState(a.leftMap(UntrackedStepException(_)), NonEmptyList.one(None))))
      })
    private def create[A](name: String, f: Span[F] => F[A]): Monadic[A] =
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
              eoa <- batchTracer.tracer.span(name).use(f).attempt
              end <- F.monotonic
            } yield {
              val completed = JobState(JobRecord(job, start, end, eoa.isRight), eoa.as(Json.Null))
              JobCursor(index + 1, end) ->
                ExecutionState(eoa = eoa, history = NonEmptyList.one(Some(completed)))
            }
          }
        }
      )
    def apply[A](name: String, fa: F[A]): Monadic[A] =
      create[A](name, _ => fa)
    def apply[A](name: String, f: Span[F] => F[A]): Monadic[A] =
      create[A](name, f)

  end JobBuilder
end BatchTraced

final class BatchTraced[F[_]: Async] private[guard] (
  scope: MetricScope,
  batchIdGenerator: F[BatchId],
  batchTracer: BatchTracer[F]):

  def sequential[A](fas: (String, Span[F] => F[A])*): BatchTraced.Sequential[F, A] = {
    val jobs = fas.toList.zipWithIndex.map { case ((name, f), idx) =>
      JobNameIndex[F, A](name, idx + 1, batchTracer.tracer.span(name).use(f))
    }
    new BatchTraced.Sequential[F, A](_ => true, scope, jobs, batchIdGenerator, batchTracer)
  }

  def parallel[A](parallelism: Int)(fas: (String, Span[F] => F[A])*): BatchTraced.Parallel[F, A] = {
    require(parallelism > 0, s"parallelism must be > 0, but was $parallelism")
    val jobs = fas.toList.zipWithIndex.map { case ((name, f), idx) =>
      JobNameIndex[F, A](name, idx + 1, batchTracer.tracer.span(name).use(f))
    }
    new BatchTraced.Parallel[F, A](_ => true, scope, parallelism, jobs, batchIdGenerator, batchTracer)
  }

  def parallel[A](fas: (String, Span[F] => F[A])*): BatchTraced.Parallel[F, A] =
    parallel[A](math.max(1, fas.size))(fas*)

  def monadic[A](f: BatchTraced.JobBuilder[F] => A): A = {
    val builder = new BatchTraced.JobBuilder[F](scope, batchIdGenerator, batchTracer)
    f(builder)
  }
end BatchTraced
