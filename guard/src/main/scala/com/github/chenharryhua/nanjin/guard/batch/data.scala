package com.github.chenharryhua.nanjin.guard.batch

import cats.derived.derived
import cats.syntax.show.{showInterpolator, toShow}
import cats.{Functor, Order, Show}
import com.github.chenharryhua.nanjin.common.DurationFormatter.defaultFormatter as fmt
import com.github.chenharryhua.nanjin.common.OpaqueLift
import com.github.chenharryhua.nanjin.guard.metrics.MetricScope
import io.circe.syntax.EncoderOps
import io.circe.{Decoder, Encoder, Json}
import org.typelevel.otel4s.trace.SpanContext

import java.time.Duration
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.ScalaDurationOps
import scala.util.control.NoStackTrace

/** Raised when a batch job completes, but the post-condition predicate rejects the value. */
final case class PostConditionUnsatisfied(job: Option[Job]) extends Exception(job match {
      case Some(value) => s"predicate failed after: ${value.displayName}"
      case None        => "untracked job predicate failed"
    }) with NoStackTrace

/** Wraps the failure of an `untracked` (lifted, jobless) step so that, once it short-circuits the chain, it
  * is distinguishable in the batch `result` from a tracked job's failure. The original exception is kept as
  * the cause.
  */
final case class UntrackedStepException(cause: Throwable) extends Exception(cause) with NoStackTrace

/** Selects whether a sequential or parallel batch retains job outcomes or raises on failure.
  *
  * Monadic jobs do not have a `BatchKind`.
  */
enum BatchKind:
  /** Retain each job's value or failure in the batch outcomes. */
  case Quasi

  /** Return successful values and raise if a job fails or fails its post-condition. */
  case Value
end BatchKind
object BatchKind:
  given Encoder[BatchKind] = Encoder.encodeString.contramap(_.productPrefix)
  given Show[BatchKind] = _.productPrefix
end BatchKind

/** Execution strategy used by a batch. */
enum BatchMode:
  case Parallel(parallelism: Int)
  case Sequential
  case Monadic
end BatchMode
object BatchMode:
  given Show[BatchMode] = {
    case Parallel(parallelism) => s"Parallel-$parallelism"
    case Sequential            => "Sequential"
    case Monadic               => "Monadic"
  }
  given Encoder[BatchMode] = Encoder.encodeString.contramap(_.show)
end BatchMode

/** Per-service-instance batch identifier.
  *
  * IDs increase from 1 as batches are launched. They are not globally unique; correlate IDs across service
  * instances using the enclosing service identity.
  */
opaque type BatchId = Long
object BatchId:
  def apply(value: Long): BatchId = value
  extension (b: BatchId)
    inline def value: Long = b
    def entry: (String, Json) = "id" -> Json.fromLong(b)

  given Show[BatchId] = OpaqueLift.lift[BatchId, Long, Show]
  given Order[BatchId] = OpaqueLift.lift[BatchId, Long, Order]
  given Ordering[BatchId] = Order[BatchId].toOrdering
  given Encoder[BatchId] = OpaqueLift.lift[BatchId, Long, Encoder]
  given Decoder[BatchId] = OpaqueLift.lift[BatchId, Long, Decoder]
end BatchId

private def batchEntry(mode: BatchMode, kind: Option[BatchKind], scope: MetricScope): (String, Json) =
  kind.fold(show"$mode Batch" -> Json.fromString(scope.label.value))(k =>
    show"$mode $k Batch" -> Json.fromString(scope.label.value))

/** Identity and batch metadata for one job.
  *
  * `index` is the job's 1-based position. `kind` is absent for monadic jobs.
  */
final case class Job(
  name: String,
  index: Int,
  scope: MetricScope,
  mode: BatchMode,
  kind: Option[BatchKind],
  batchId: BatchId):
  /** Human-readable name combining the job index and configured name. */
  val displayName: String = s"job-$index $name"
  val nameEntry: (String, Json) = s"job-$index" -> Json.fromString(name)
end Job
object Job {
  given Encoder[Job] = Encoder.instance { (a: Job) =>
    Json.obj(a.nameEntry, batchEntry(a.mode, a.kind, a.scope), a.batchId.entry)
  }
}

/** Classification of a completed job, interpreted together with `JobState.result`.
  *
  *   - `Accepted`: the job met its post-condition, or a monadic failure was caught by `attempt`.
  *   - `Unmet`: a quasi-batch retained a rejected value or failure, or a monadic `predicate` rejected a
  *     value.
  *   - `Failed`: a job or monadic filter failure stopped execution, or a value-batch post-condition failed.
  *
  * A caught failure may still have a `Left` result; check both fields or use `succeeded` to determine whether
  * the job passed.
  */
enum JobFlag:
  case Accepted, Unmet, Failed

/** Identity, timing, and optional trace context for a completed job.
  *
  * Times use a monotonic clock. In monadic batches, each interval starts at the previous job's end, or at the
  * batch start for the first job, so it may include untracked work.
  *
  * @param job
  *   the job metadata this record describes
  * @param start
  *   monotonic clock reading at the start of the interval
  * @param end
  *   monotonic clock reading at the end of the interval
  * @param spanContext
  *   the tracing span context of the job's own span, present only for traced batches (`batchTraced`); `None`
  *   for the untraced `batch`/`batchMetered` front ends, which open no span
  */
final case class JobRecord(
  job: Job,
  start: FiniteDuration,
  end: FiniteDuration,
  spanContext: Option[SpanContext]) {

  /** Elapsed time for this job, derived as `end - start`. */
  val took: Duration = (end - start).toJava

  /** The W3C Trace Context `traceparent` header value for this job's span, present only when `spanContext`
    * is. Format: `00-<32-hex traceId>-<16-hex spanId>-<2-hex flags>`. See
    * https://www.w3.org/TR/trace-context/#traceparent-header
    */
  val traceparent: Option[String] =
    spanContext.map(ctx => s"00-${ctx.traceIdHex}-${ctx.spanIdHex}-${ctx.traceFlags.toHex}")
}

/** The recorded outcome of a single batch job: the completed job summary, its classification, and its result.
  *
  * @param record
  *   the job's identity and timing
  * @param flag
  *   how the job is classified; see `JobFlag`
  * @param result
  *   the job's result, or the replacement JSON set by a monadic `renderOutcome`
  */
final case class JobState[A](record: JobRecord, flag: JobFlag, result: Either[Throwable, A]) derives Functor {

  /** True only when the job is `Accepted` and its result is a `Right`. */
  val succeeded: Boolean = result.isRight && flag.match {
    case JobFlag.Accepted => true
    case JobFlag.Failed   => false
    case JobFlag.Unmet    => false
  }
}

sealed trait BatchResult {
  protected type S // state type
  protected type R // result type

  /** Metric scope (label and domain) this batch was run under. */
  def scope: MetricScope

  /** Elapsed time for the batch's execution.
    *
    * Sequential and parallel batches time the job traversal. Monadic batches time the whole chain, including
    * untracked steps, and report elapsed time even when no named jobs run.
    */
  def spent: Duration

  /** Sequential, parallel, or monadic execution mode. */
  def mode: BatchMode

  /** Identifier for this batch execution. See `BatchId` for the full semantics. */
  def batchId: BatchId

  /** Recorded outcomes for jobs that ran. Monadic outcomes use JSON, defaulting to `Json.Null` unless
    * rendered.
    */
  def outcomes: List[JobState[S]]

  /** Aggregate result: `Unit` for a quasi-batch, successful values for a value-batch, or the final `Either`
    * for a monadic batch.
    */
  def result: R

  /** Whether all recorded jobs succeeded. */
  final def allSucceeded: Boolean = outcomes.forall(_.succeeded)
}

/** Result of a quasi-batch; per-job values and failures are retained in `outcomes`. */
final case class QuasiBatch[A](
  scope: MetricScope,
  spent: Duration,
  mode: BatchMode,
  batchId: BatchId,
  outcomes: List[JobState[A]])
    extends BatchResult derives Functor {
  override val result: Unit = ()
  override protected type S = A
  override protected type R = Unit
}
object QuasiBatch:
  // Showing the produced value under its outcome tag is safe here: this encoder runs only when the user
  // chooses to serialize the returned batch, unlike the auto-emitted per-job log (see `JobLog`). Same reason
  // the `Encoder[A]` is required only at these user-triggered encoders, not on the batch builders.
  given [A: Encoder] => Encoder[QuasiBatch[A]] =
    Encoder.instance { qb =>
      val failed = qb.outcomes.count(!_.succeeded)
      Json.obj(
        batchEntry(qb.mode, Some(BatchKind.Quasi), qb.scope),
        qb.batchId.entry,
        JobLog.SPENT -> Json.fromString(fmt.format(qb.spent)),
        JobLog.FAILED -> Json.fromInt(failed),
        JobLog.JOBS -> qb.outcomes.map(js => toLogEntry(js).message.inBatch).asJson
      )
    }
end QuasiBatch

/** Result of a value-batch; `result` contains the successful values and `outcomes` their metadata. */
final case class ValueBatch[A](
  scope: MetricScope,
  spent: Duration,
  mode: BatchMode,
  batchId: BatchId,
  outcomes: List[JobState[A]],
  result: List[A])
    extends BatchResult derives Functor {
  override protected type S = A
  override protected type R = List[A]
}

object ValueBatch:
  given [A: Encoder] => Encoder[ValueBatch[A]] =
    Encoder.instance { bv =>
      Json.obj(
        batchEntry(bv.mode, Some(BatchKind.Value), bv.scope),
        bv.batchId.entry,
        JobLog.SPENT -> Json.fromString(fmt.format(bv.spent)),
        JobLog.JOBS -> bv.outcomes.map(js => toLogEntry(js).message.inBatch).asJson
      )
    }
end ValueBatch

/** Result of a monadic batch, with recorded named-job outcomes and the final chain result.
  *
  * `result` is `Left` when a failure stopped the chain, or `Right(value)` on success. `attempt` can capture a
  * failure so the chain continues, in which case the error is part of the value. `spent` includes untracked
  * work and is meaningful even when the chain ran no named jobs.
  */
final case class MonadicBatch[A](
  scope: MetricScope,
  spent: Duration,
  batchId: BatchId,
  outcomes: List[JobState[Json]],
  result: Either[Throwable, A])
    extends BatchResult derives Functor {
  override protected type S = Json
  override protected type R = Either[Throwable, A]
  override val mode: BatchMode = BatchMode.Monadic
}

object MonadicBatch:
  given [A: Encoder] => Encoder[MonadicBatch[A]] =
    Encoder.instance { mb =>
      val base: Json = Json.obj(
        batchEntry(mb.mode, None, mb.scope),
        mb.batchId.entry,
        JobLog.SPENT -> Json.fromString(fmt.format(mb.spent)),
        JobLog.JOBS -> mb.outcomes.map(js => toLogEntry(js).message.inBatch).asJson
      )
      mb.result.fold(_ => base, r => Json.obj("result" -> r.asJson).deepMerge(base))
    }
end MonadicBatch
