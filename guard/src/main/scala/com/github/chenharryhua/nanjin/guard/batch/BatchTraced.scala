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

  /** A traced job: its display name, 1-based position in the batch, and a function that produces the job's
    * effect given the job's own span. `BatchRunner.runTraced` opens that child span (named after the job),
    * passes it to `run`, and records the span context on the job's `JobRecord`.
    */
  final private[BatchTraced] case class SpanJob[F[_], A](name: String, index: Int, run: Span[F] => F[A])

  /*
   * Runners
   */

  /** Common runner operations for the sequential and parallel traced batches.
    *
    * As in the other two façades the two shapes differ only in how they traverse the job list, so that one
    * choice is the abstract `traverseJobs`. This runner owns the span lifecycle (`runTraced`) and its own
    * copy of the quasi and value classification rules, so it does not go through `JobExecutor`.
    */
  sealed abstract protected class BatchRunner[F[_], A](using F: Async[F]) {

    protected def mode: BatchMode

    protected def scope: MetricScope

    protected def jobs: List[SpanJob[F, A]]

    protected def batchIdGenerator: F[BatchId]

    protected def predicate: A => Boolean

    protected def traverseJobs[B](f: SpanJob[F, A] => F[B]): F[List[B]]

    protected def batchTracer: BatchTracer[F]

    /** Set the post-condition for successful jobs. Quasi-batches retain rejected values as unmet;
      * value-batches fail with `PostConditionUnsatisfied`.
      */
    def withPostCondition(f: A => Boolean): BatchRunner[F, A]

    /** Static metadata for a traced job of the given `kind`. */
    private def makeJob(kind: BatchKind, sj: SpanJob[F, A], batchId: BatchId): Job =
      Job(sj.name, sj.index, scope, mode, Some(kind), batchId)

    /** Run `sj` inside its own child span (named after the job) and record the span context, timing the
      * `attempt`ed effect. The child span nests under `batchTracer.parent`, whose scope the callers enter.
      * The classification of `eoa` is left to the caller, matching the quasi/value split; `BatchTraced`
      * carries its own copy of those rules rather than sharing `JobExecutor`'s.
      *
      * The timing window brackets the span, so `took` includes opening and closing it, matching the monadic
      * traced path (whose cursor-threaded `start` cannot exclude it) and the framing that `BatchMetered`
      * already counts for its own jobs. See `JobRecord` for the resulting semantics.
      */
    private def runTraced(job: Job, sj: SpanJob[F, A]): F[(JobRecord, Either[Throwable, A])] =
      for {
        start <- F.monotonic
        (ctx, eoa) <- batchTracer.tracer
          .span(sj.name)
          .use(span => sj.run(span).attempt.map(span.context -> _))
        end <- F.monotonic
      } yield (JobRecord(job, start, end, Some(ctx)), eoa)

    /** A quasi job: a predicate miss is flagged `JobFlag.Unmet` but keeps the value as a `Right`; a thrown
      * effect stays a `Left`, also flagged `JobFlag.Unmet`.
      */
    private def quasiJob(sj: SpanJob[F, A], batchId: BatchId): F[JobState[A]] =
      runTraced(makeJob(BatchKind.Quasi, sj, batchId), sj).map { case (record, eoa) =>
        val flag = eoa.fold(_ => JobFlag.Unmet, v => if predicate(v) then JobFlag.Accepted else JobFlag.Unmet)
        JobState(record, flag, eoa)
      }

    /** A value job: a predicate miss folds into `Left(PostConditionUnsatisfied)` so the value batch can raise
      * it and abort; any `Left` is flagged `JobFlag.Failed`, a passing value `JobFlag.Accepted`.
      */
    private def valueJob(sj: SpanJob[F, A], batchId: BatchId): F[JobState[A]] =
      runTraced(makeJob(BatchKind.Value, sj, batchId), sj).map { case (record, eoa) =>
        val result =
          eoa.flatMap(a => if (predicate(a)) Right(a) else Left(PostConditionUnsatisfied(Some(record.job))))
        JobState(record, result.fold(_ => JobFlag.Failed, _ => JobFlag.Accepted), result)
      }

    /** Execute inside the batch's parent span while preserving per-job success or failure state.
      *
      * A job that throws is captured as a failed job result, so the batch runs to completion and reports
      * every outcome.
      */
    final def quasiBatch: F[QuasiBatch[A]] =
      batchIdGenerator.flatMap { batchId =>
        batchTracer.parent.surround {
          F.timed(traverseJobs(sj => quasiJob(sj, batchId))).map {
            case (fd: FiniteDuration, js: List[JobState[A]]) =>
              QuasiBatch(scope = scope, spent = fd.toJava, mode = mode, batchId = batchId, outcomes = js)
          }
        }
      }

    /** Execute inside the batch's parent span and raise on failure, returning the successful values.
      *
      * A job that throws, or whose value misses the post-condition, is raised as `PostConditionUnsatisfied`
      * or the original exception, which aborts the batch and marks the parent span as errored.
      */
    final def valueBatch: F[ValueBatch[A]] =
      batchIdGenerator.flatMap { batchId =>
        batchTracer.parent.surround {
          F.timed(traverseJobs { sj =>
            valueJob(sj, batchId).map(js => js.result.map(JobValue(js.record, _))).rethrow
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
  }

  /*
   * Parallel
   */
  final class Parallel[F[_], A] private[BatchTraced] (
    protected val predicate: A => Boolean,
    protected val scope: MetricScope,
    parallelism: Int,
    protected val jobs: List[SpanJob[F, A]],
    protected val batchIdGenerator: F[BatchId],
    protected val batchTracer: BatchTracer[F]
  )(using F: Async[F])
      extends BatchRunner[F, A] {

    override protected val mode: BatchMode = BatchMode.Parallel(parallelism)

    override protected def traverseJobs[B](f: SpanJob[F, A] => F[B]): F[List[B]] =
      F.parTraverseN(parallelism)(jobs)(f)

    override def withPostCondition(f: A => Boolean): Parallel[F, A] =
      new Parallel[F, A](predicate = f, scope, parallelism, jobs, batchIdGenerator, batchTracer)
  }

  /*
   * Sequential
   */
  final class Sequential[F[_], A] private[BatchTraced] (
    protected val predicate: A => Boolean,
    protected val scope: MetricScope,
    protected val jobs: List[SpanJob[F, A]],
    protected val batchIdGenerator: F[BatchId],
    protected val batchTracer: BatchTracer[F])(using F: Async[F])
      extends BatchRunner[F, A] {

    override protected val mode: BatchMode = BatchMode.Sequential

    override protected def traverseJobs[B](f: SpanJob[F, A] => F[B]): F[List[B]] =
      jobs.traverse(f)

    override def withPostCondition(f: A => Boolean): Sequential[F, A] =
      new Sequential[F, A](predicate = f, scope, jobs, batchIdGenerator, batchTracer)
  }

  /*
   * Monadic
   */

  /** Builder for traced monadic batches whose jobs are composed with `map` and `flatMap`. Each job runs in
    * its own child span, and a job may take that span to attach attributes or open spans beneath it.
    */
  final class JobBuilder[F[_]] private[BatchTraced] (
    scope: MetricScope,
    batchIdGenerator: F[BatchId],
    batchTracer: BatchTracer[F])(using F: Async[F]):

    final class Monadic[A] private[BatchTraced] (
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

    /** Add a value to the chain without recording a job or creating a span. */
    def pure[A](a: A): Monadic[A] =
      new Monadic[A](Kleisli { _ =>
        StateT(cursor => (cursor -> ExecutionState(Right(a), NonEmptyList.one(None))).pure[F])
      })

    /** Add an untracked effect without creating a span. Its failure stops the chain and is captured in the
      * batch result.
      */
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
              (ctx, eoa) <- batchTracer.tracer.span(name).use(span => f(span).attempt.map(span.context -> _))
              end <- F.monotonic
            } yield {
              val flag = if (eoa.isRight) JobFlag.Accepted else JobFlag.Failed
              val completed = JobState(JobRecord(job, start, end, Some(ctx)), flag, eoa.as(Json.Null))
              JobCursor(index + 1, end) ->
                ExecutionState(eoa = eoa, history = NonEmptyList.one(Some(completed)))
            }
          }
        }
      )

    /** Add a named job that runs in its own child span. A failure stops later segments. */
    def apply[A](name: String, fa: F[A]): Monadic[A] =
      create[A](name, _ => fa)

    /** Add a named job whose effect receives its child span. */
    def apply[A](name: String, f: Span[F] => F[A]): Monadic[A] =
      create[A](name, f)

  end JobBuilder
end BatchTraced

/** Batch API that runs each job in its own OpenTelemetry span.
  *
  * Use sequential or parallel execution for independent jobs, and monadic execution when later jobs depend on
  * earlier results. Each effect receives its child span and may use it to add attributes or create nested
  * spans. Results are returned directly in `F`; use `quasiBatch` to retain job failures or `valueBatch` to
  * raise them. This API creates spans but does not add lifecycle logs or progress metrics.
  */
final class BatchTraced[F[_]: Async] private[guard] (
  scope: MetricScope,
  batchIdGenerator: F[BatchId],
  batchTracer: BatchTracer[F]):

  /** Create a batch that runs the named span-aware effects in order. */
  def sequential[A](fas: (String, Span[F] => F[A])*): BatchTraced.Sequential[F, A] = {
    val jobs = fas.toList.zipWithIndex.map { case ((name, f), idx) =>
      BatchTraced.SpanJob[F, A](name, idx + 1, f)
    }
    new BatchTraced.Sequential[F, A](_ => true, scope, jobs, batchIdGenerator, batchTracer)
  }

  /** Create a batch that runs named span-aware effects concurrently up to `parallelism`.
    *
    * Requires `parallelism > 0`; otherwise this method throws `IllegalArgumentException`.
    */
  def parallel[A](parallelism: Int)(fas: (String, Span[F] => F[A])*): BatchTraced.Parallel[F, A] = {
    require(parallelism > 0, s"parallelism must be > 0, but was $parallelism")
    val jobs = fas.toList.zipWithIndex.map { case ((name, f), idx) =>
      BatchTraced.SpanJob[F, A](name, idx + 1, f)
    }
    new BatchTraced.Parallel[F, A](_ => true, scope, parallelism, jobs, batchIdGenerator, batchTracer)
  }

  /** Create a parallel batch with concurrency set to the number of jobs, or one when the list is empty. */
  def parallel[A](fas: (String, Span[F] => F[A])*): BatchTraced.Parallel[F, A] =
    parallel[A](math.max(1, fas.size))(fas*)

  /** Build a chain of traced jobs whose later steps may depend on earlier results. */
  def monadic[A](f: BatchTraced.JobBuilder[F] => A): A = {
    val builder = new BatchTraced.JobBuilder[F](scope, batchIdGenerator, batchTracer)
    f(builder)
  }
end BatchTraced
