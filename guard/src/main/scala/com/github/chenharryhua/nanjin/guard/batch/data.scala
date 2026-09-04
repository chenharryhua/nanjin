package com.github.chenharryhua.nanjin.guard.batch

import cats.derived.derived
import cats.syntax.show.{showInterpolator, toShow}
import cats.{Functor, Show}
import com.github.chenharryhua.nanjin.common.DurationFormatter.defaultFormatter as fmt
import com.github.chenharryhua.nanjin.guard.config.StackTrace
import com.github.chenharryhua.nanjin.guard.metrics.MetricScope
import io.circe.syntax.EncoderOps
import io.circe.{Encoder, Json}

import java.time.Duration
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

/** Metadata describing a single batch step and the execution context in which it ran.
  *
  * @param batchId
  *   identifier of the batch this job belongs to; a per-service-instance monotonic counter starting at 1. See
  *   [[BatchResult.batchId]] for the full semantics.
  */
final case class Job(
  name: String,
  index: Int,
  scope: MetricScope,
  mode: BatchMode,
  kind: BatchKind,
  batchId: Long):
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

/** A completed job record that captures its identity, elapsed time, and whether it finished successfully. */
final case class CompletedJob(job: Job, took: Duration, succeeded: Boolean)

/** The recorded outcome of a single batch job, including the completed job summary and its result. */
final case class JobState[A](record: CompletedJob, result: Either[Throwable, A]) derives Functor {
  val succeeded: Boolean = result.isRight
}
object JobState:
  given [A: Encoder] => Encoder[JobState[A]] = Encoder.instance { a =>
    Json.obj("took" -> Json.fromString(fmt.format(a.record.took)), resultTag(a.succeeded) -> a.result.asJson)
      .deepMerge(a.record.job.asJson)
  }

/** A successful batch job value paired with the completion metadata for that job. */
final case class JobValue[A](record: CompletedJob, result: A) derives Functor
object JobValue:
  given [A: Encoder] => Encoder[JobValue[A]] = Encoder.instance { a =>
    Json.obj(
      "took" -> Json.fromString(fmt.format(a.record.took)),
      resultTag(a.record.succeeded) -> a.result.asJson)
      .deepMerge(a.record.job.asJson)
  }

/** Summary of all jobs completed by a batch execution. */
final case class CompletedBatch(
  scope: MetricScope,
  spent: Duration,
  mode: BatchMode,
  batchId: Long,
  jobs: List[CompletedJob]) {

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

  /** Total elapsed execution time. */
  def spent: Duration

  /** Sequential, parallel, or monadic execution mode. */
  def mode: BatchMode

  /** Identifier for this batch execution.
    *
    * A monotonic counter, starting at 1, minted from a single `AtomicLong` created per service instance.
    * Successive batches within the same instance receive 1, 2, 3, … in the order they are launched, so the
    * latest value also reveals how many batches the instance has run.
    *
    * The id is unique '''within a service instance''', not globally: a new instance (a new `serviceId`, e.g.
    * on redeploy) starts its counter over at 1. Cross-instance correlation therefore relies on the enclosing
    * event's `serviceId`, which is why the id is a plain counter rather than a random UUID. It is emitted as
    * the JSON number `batch_id`.
    */
  def batchId: Long

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
  batchId: Long,
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
  batchId: Long,
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
  */
final case class MonadicBatch[A](
  scope: MetricScope,
  spent: Duration,
  batchId: Long,
  jobs: List[CompletedJob],
  result: Either[Throwable, A])
    extends BatchResult[CompletedJob] derives Functor {
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
