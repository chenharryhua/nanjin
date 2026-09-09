package com.github.chenharryhua.nanjin.guard.batch

import cats.derived.derived
import cats.syntax.show.{showInterpolator, toShow}
import cats.{Functor, Order, Show}
import com.github.chenharryhua.nanjin.common.DurationFormatter.defaultFormatter as fmt
import com.github.chenharryhua.nanjin.common.OpaqueLift
import com.github.chenharryhua.nanjin.guard.config.StackTrace
import com.github.chenharryhua.nanjin.guard.metrics.MetricScope
import io.circe.syntax.EncoderOps
import io.circe.{Decoder, Encoder, Json}
import org.apache.commons.lang3.exception.ExceptionUtils

import java.time.Duration
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.ScalaDurationOps
import scala.util.control.NoStackTrace

/** Raised when a batch job completes, but the post-condition predicate rejects the value. */
final case class PostConditionUnsatisfied(job: Option[Job]) extends Exception(job match {
      case Some(value) => s"predicate failed after: ${value.displayName}"
      case None        => "predicate failed before: job-1"
    }) with NoStackTrace

/** Distinguishes the two batch execution shapes: quasi-batches expose per-job outcome state, while
  * value-batches carry the successful result values for each completed job.
  */
enum BatchKind:
  /** Collects each job outcome, including failures, in the resulting quasi-batch. */
  case Quasi

  /** Propagates a job failure and returns only successful values. */
  case Value
end BatchKind
object BatchKind:
  given Encoder[BatchKind] = Encoder.encodeString.contramap(_.productPrefix)
  given Show[BatchKind] = _.productPrefix
end BatchKind

/** Describes how a batch is executed. */
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

/** Identifier for a single batch execution.
  *
  * A monotonic counter, starting at 1, minted from a single `AtomicLong` created per service instance (the
  * `batchIdGenerator`). Successive batches within the same instance receive 1, 2, 3, … in the order they are
  * launched, so the latest value also reveals how many batches the instance has run.
  *
  * The id is unique '''within a service instance''', not globally: a new instance (a new `serviceId`, e.g. on
  * redeploy) starts its counter over at 1. Cross-instance correlation therefore relies on the enclosing
  * event's `serviceId`, which is why the id is a plain counter rather than a random UUID. It is emitted as
  * the JSON number `batch_id`.
  */
opaque type BatchId = Long
object BatchId:
  def apply(value: Long): BatchId = value
  extension (b: BatchId) inline def value: Long = b

  given Show[BatchId] = OpaqueLift.lift[BatchId, Long, Show]
  given Order[BatchId] = OpaqueLift.lift[BatchId, Long, Order]
  given Ordering[BatchId] = Order[BatchId].toOrdering
  given Encoder[BatchId] = OpaqueLift.lift[BatchId, Long, Encoder]
  given Decoder[BatchId] = OpaqueLift.lift[BatchId, Long, Decoder]
end BatchId

/** Metadata describing a single batch step and the execution context in which it ran.
  *
  * @param batchId
  *   identifier of the batch this job belongs to; a per-service-instance monotonic counter starting at 1. See
  *   `BatchResult.batchId` for the full semantics.
  */
final case class Job(
  name: String,
  index: Int,
  scope: MetricScope,
  mode: BatchMode,
  kind: BatchKind,
  batchId: BatchId):
  val batch: String = scope.label.value
  val domain: String = scope.domain.value

  /** Human-readable name combining the job index and configured name. */
  def displayName: String = s"job-$index $name"
end Job
object Job {
  given Encoder[Job] = Encoder.instance { (a: Job) =>
    Json.obj(
      show"job-${a.index}" -> Json.fromString(a.name),
      "batch" -> Json.fromString(a.batch),
      "batch_id" -> a.batchId.asJson,
      "domain" -> Json.fromString(a.domain),
      "mode" -> a.mode.asJson,
      "kind" -> a.kind.asJson
    )
  }
}

/** A completed job record that captures its identity, timing boundaries, and whether it finished
  * successfully.
  *
  * `start` and `end` are `monotonic` readings taken around the job's execution; `took` is derived as
  * `end - start`. In `Batch` the window brackets the job's own kickoff log and its effect, so `took` includes
  * the kickoff; in `BatchLight` there is no kickoff log, so `took` is the effect alone. Either way the
  * completion log (when present) is written after `end` and is therefore not part of `took`. Kickoff and
  * completion are internal, non-throwing framework log writes, so their cost is negligible: the batch `spent`
  * is at least the sum of the per-job `took`s, but the difference (completion logging plus batch framing) is
  * tiny in practice, not a place where meaningful time hides. Apart from that negligible kickoff delta,
  * `Batch` and `BatchLight` share the same timing model.
  *
  * For monadic batches the boundaries are post-processed (see `monadicHistory`, shared by both `Batch` and
  * `BatchLight`) so that a job's `took` also absorbs the wall-clock spent before it that belongs to no job of
  * its own — chiefly preceding invisible `lift`/`pure` steps (and, negligibly, the previous job's completion
  * log). This keeps the per-job durations contiguous and summing exactly to the batch `spent`.
  *
  * @param job
  *   the job metadata this record describes
  * @param start
  *   monotonic clock reading at the start of the job (or the previous job's `end`, after monadic
  *   redistribution)
  * @param end
  *   monotonic clock reading at the end of the job
  * @param succeeded
  *   whether the job completed successfully and satisfied its post-condition
  */
final case class JobRecord(job: Job, start: FiniteDuration, end: FiniteDuration, succeeded: Boolean) {

  /** Elapsed time for this job, derived as `end - start`. */
  val took: Duration = (end - start).toJava
}

/** The recorded outcome of a single batch job, including the completed job summary and its result. */
final case class JobState[A](record: JobRecord, result: Either[Throwable, A]) derives Functor {
  val succeeded: Boolean = result.isRight
}

/** A successful batch job value paired with the completion metadata for that job. */
final case class JobValue[A](record: JobRecord, result: A) derives Functor

/** Summary of all jobs completed by a batch execution. */
final case class CompletedBatch(
  scope: MetricScope,
  spent: Duration,
  mode: BatchMode,
  batchId: BatchId,
  jobs: List[JobRecord]) {

  /** Whether every job in the batch completed successfully. */
  def succeeded: Boolean = jobs.forall(_.succeeded)
}
object CompletedBatch:
  given Encoder[CompletedBatch] =
    Encoder.instance { cb =>
      val (succeeded, failed) = cb.jobs.partition(_.succeeded)
      Json.obj(
        "batch" -> cb.scope.label.asJson,
        "batch_id" -> cb.batchId.asJson,
        "domain" -> Json.fromString(cb.scope.domain.value),
        "mode" -> cb.mode.asJson,
        "spent" -> Json.fromString(fmt.format(cb.spent)),
        "succeeded" -> Json.fromInt(succeeded.length),
        "failed" -> Json.fromInt(failed.length),
        "jobs" -> cb.jobs.map(cj =>
          Json.obj(
            show"job-${cj.job.index}" -> Json.fromString(cj.job.name),
            "took" -> Json.fromString(fmt.format(cj.took)),
            "kind" -> cj.job.kind.asJson,
            "succeeded" -> Json.fromBoolean(cj.succeeded)
          ))
          .asJson
      )
    }

sealed trait BatchResult[A] {

  /** Metric scope (label and domain) this batch was run under. */
  def scope: MetricScope

  /** Total elapsed execution time, measured from the first job onward.
    *
    * The clock starts when the first job starts, so any work performed before it is not counted: pre-batch
    * `IO` for sequential and parallel batches, or a leading `lift`/`pure` step for monadic batches. For
    * sequential and parallel this is measured directly around job execution; for monadic it is the span from
    * the first job's start to the last job's end (see `MonadicBatch`).
    */
  def spent: Duration

  /** Sequential, parallel, or monadic execution mode. */
  def mode: BatchMode

  /** Identifier for this batch execution. See `BatchId` for the full semantics. */
  def batchId: BatchId

  /** Per-job result values represented by this result type. */
  def jobs: List[A]

  /** Whether all jobs completed successfully. */
  def succeeded: Boolean

  /** Completion-only summary suitable for reporting. */
  def summary: CompletedBatch
}

/** The aggregate result of a quasi-batch execution, where each job contributes a completion record and
  * outcome state.
  */
final case class QuasiBatch[A](
  scope: MetricScope,
  spent: Duration,
  mode: BatchMode,
  batchId: BatchId,
  jobs: List[JobState[A]])
    extends BatchResult[JobState[A]] derives Functor {
  override def succeeded: Boolean = jobs.forall(_.record.succeeded)
  override def summary: CompletedBatch = CompletedBatch(
    scope = scope,
    spent = spent,
    mode = mode,
    batchId = batchId,
    jobs = jobs.map(_.record)
  )
}
object QuasiBatch:
  given [A: Encoder] => Encoder[QuasiBatch[A]] =
    Encoder.instance { qb =>
      val (succeeded, failed) = qb.jobs.partition(_.record.succeeded)
      Json.obj(
        "batch" -> qb.scope.label.asJson,
        "batch_id" -> qb.batchId.asJson,
        "domain" -> Json.fromString(qb.scope.domain.value),
        "mode" -> qb.mode.asJson,
        "kind" -> BatchKind.Quasi.asJson,
        "spent" -> Json.fromString(fmt.format(qb.spent)),
        "succeeded" -> Json.fromInt(succeeded.length),
        "failed" -> Json.fromInt(failed.length),
        "jobs" -> qb.jobs.map { js =>
          Json.obj(
            show"job-${js.record.job.index}" -> Json.fromString(js.record.job.name),
            "took" -> Json.fromString(fmt.format(js.record.took)),
            resultTag(js.succeeded) -> js.result.asJson
          )
        }.asJson
      )
    }
end QuasiBatch

/** The aggregate result of a value-batch execution, where each job contributes a successful value and
  * completion metadata.
  */
final case class ValueBatch[A](
  scope: MetricScope,
  spent: Duration,
  mode: BatchMode,
  batchId: BatchId,
  jobs: List[JobValue[A]])
    extends BatchResult[JobValue[A]] derives Functor {
  override val succeeded: Boolean = true
  override def summary: CompletedBatch =
    CompletedBatch(
      scope = scope,
      spent = spent,
      mode = mode,
      batchId = batchId,
      jobs = jobs.map(_.record)
    )
}
object ValueBatch:
  given [A: Encoder] => Encoder[ValueBatch[A]] =
    Encoder.instance { bv =>
      Json.obj(
        "batch" -> bv.scope.label.asJson,
        "batch_id" -> bv.batchId.asJson,
        "domain" -> Json.fromString(bv.scope.domain.value),
        "mode" -> bv.mode.asJson,
        "kind" -> BatchKind.Value.asJson,
        "spent" -> Json.fromString(fmt.format(bv.spent)),
        "jobs" -> bv.jobs.map(js =>
          Json.obj(
            show"job-${js.record.job.index}" -> Json.fromString(js.record.job.name),
            "took" -> Json.fromString(fmt.format(js.record.took)),
            resultTag(js.record.succeeded) -> js.result.asJson
          ))
          .asJson
      )
    }
end ValueBatch

/** The aggregate result of a monadic batch execution, including the recorded step history and final result.
  *
  * `spent` is the wall-clock span from the first job's start to the last job's end, so it includes the time
  * consumed by invisible `lift`/`pure` steps between jobs. Each recorded job's `took` is adjusted to absorb
  * the preceding gap (see `JobRecord`), so the per-job durations sum to `spent`.
  */
final case class MonadicBatch[A](
  scope: MetricScope,
  spent: Duration,
  batchId: BatchId,
  jobs: List[JobRecord],
  result: Either[Throwable, A])
    extends BatchResult[JobRecord] derives Functor {
  override val mode: BatchMode = BatchMode.Monadic
  override def succeeded: Boolean = result.isRight

  override def summary: CompletedBatch =
    CompletedBatch(
      scope = scope,
      spent = spent,
      mode = BatchMode.Monadic,
      batchId = batchId,
      jobs = jobs
    )
}
object MonadicBatch:
  given [A: Encoder] => Encoder[MonadicBatch[A]] =
    Encoder.instance { mb =>
      Json.obj(
        "batch" -> mb.scope.label.asJson,
        "batch_id" -> mb.batchId.asJson,
        "domain" -> Json.fromString(mb.scope.domain.value),
        "mode" -> mb.mode.asJson,
        "spent" -> Json.fromString(fmt.format(mb.spent)),
        "jobs" -> mb.jobs.map { cj =>
          if (cj.succeeded)
            Json.obj(
              show"job-${cj.job.index}" -> Json.fromString(cj.job.name),
              "took" -> Json.fromString(fmt.format(cj.took)))
          else {
            val severity = cj.job.kind match {
              case BatchKind.Quasi => Json.fromString(SeverityNonFatal)
              case BatchKind.Value => Json.fromString(SeverityCritical)
            }
            Json.obj(
              show"job-${cj.job.index}" -> Json.fromString(cj.job.name),
              "took" -> Json.fromString(fmt.format(cj.took)),
              "failed" -> severity
            )
          }
        }
          .asJson,
        resultTag(mb.succeeded) -> mb.result.fold(StackTrace(_).asJson, _.asJson)
      )
    }
end MonadicBatch

sealed private trait JobLog
private object JobLog {
  given Encoder[JobLog] = Encoder.instance {
    case Kickoff(job)              => Json.obj("kickoff" -> job.asJson)
    case Canceled(job)             => Json.obj("canceled" -> job.asJson)
    case Succeeded(record, result) =>
      Json.obj(
        "succeeded" -> record.job.asJson,
        "took" -> Json.fromString(fmt.format(record.took)),
        "result" -> result)

    case Nonfatal(record, error) =>
      Json.obj(
        "nonfatal" -> record.job.asJson,
        "took" -> Json.fromString(fmt.format(record.took)),
        "error" -> Json.fromString(ExceptionUtils.getMessage(error)))

    case Critical(record, error) =>
      Json.obj(
        "critical" -> record.job.asJson,
        "took" -> Json.fromString(fmt.format(record.took)),
        "error" -> Json.fromString(ExceptionUtils.getMessage(error)))
  }

  final case class Kickoff(job: Job) extends JobLog
  final case class Canceled(job: Job) extends JobLog
  final case class Succeeded(record: JobRecord, result: Json) extends JobLog
  final case class Nonfatal(record: JobRecord, error: Throwable) extends JobLog
  final case class Critical(record: JobRecord, error: Throwable) extends JobLog
}
