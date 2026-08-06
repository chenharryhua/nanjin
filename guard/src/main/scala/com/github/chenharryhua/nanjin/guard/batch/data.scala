package com.github.chenharryhua.nanjin.guard.batch

import cats.derived.derived
import cats.syntax.show.{showInterpolator, toShow}
import cats.{Functor, Show}
import com.github.chenharryhua.nanjin.common.DurationFormatter.defaultFormatter as fmt
import com.github.chenharryhua.nanjin.guard.event.{MetricLabel, StackTrace}
import io.circe.syntax.EncoderOps
import io.circe.{Encoder, Json}
import org.apache.commons.lang3.exception.ExceptionUtils

import java.time.Duration
import java.util.UUID
import scala.util.control.NoStackTrace

/** Raised when a batch job completes, but the post-condition predicate rejects the value. */
final case class PostConditionUnsatisfied(job: Option[Job]) extends Exception(
      s"predicate failed after: ${job.map(_.displayName).getOrElse("<empty>")}") with NoStackTrace

/** Distinguishes the two batch execution shapes: quasi-batches expose per-job outcome state, while
  * value-batches carry the successful result values for each completed job.
  */
enum BatchKind:
  case Quasi, Value
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

/** Metadata describing a single batch step and the execution context in which it ran. */
final case class Job(
  name: String,
  index: Int,
  label: MetricLabel,
  mode: BatchMode,
  kind: BatchKind,
  batchId: UUID):
  val batch: String = label.label
  val domain: String = label.domain.value
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
final case class CompletedJob(job: Job, took: Duration, done: Boolean)

private given [A: Encoder] => Encoder[Either[Throwable, A]] =
  Encoder.instance {
    case Left(ex)     => Json.fromString(ExceptionUtils.getMessage(ex))
    case Right(value) => value.asJson
  }

/** The recorded outcome of a single batch job, including the completed job summary and its result. */
final case class JobState[A](completed: CompletedJob, result: Either[Throwable, A]) derives Functor {
  val done: Boolean = result.isRight
}
object JobState:
  given [A: Encoder] => Encoder[JobState[A]] = Encoder.instance { a =>
    Json.obj("took" -> Json.fromString(fmt.format(a.completed.took)), "result" -> a.result.asJson)
      .deepMerge(a.completed.job.asJson)
  }

/** A successful batch job value paired with the completion metadata for that job. */
final case class JobValue[A](completed: CompletedJob, result: A) derives Functor
object JobValue:
  given [A: Encoder] => Encoder[JobValue[A]] = Encoder.instance { a =>
    Json.obj("took" -> Json.fromString(fmt.format(a.completed.took)), "result" -> a.result.asJson)
      .deepMerge(a.completed.job.asJson)
  }

final case class CompletedBatch(
  label: MetricLabel,
  spent: Duration,
  mode: BatchMode,
  batchId: UUID,
  jobs: List[CompletedJob]) {
  def done: Boolean = jobs.forall(_.done)
}
object CompletedBatch:
  given Encoder[CompletedBatch] =
    Encoder.instance { cb =>
      val (done, fail) = cb.jobs.partition(_.done)
      Json.obj(
        "batch" -> Json.fromString(cb.label.label),
        "batch_id" -> cb.batchId.asJson,
        "domain" -> Json.fromString(cb.label.domain.value),
        "mode" -> cb.mode.asJson,
        "spent" -> Json.fromString(fmt.format(cb.spent)),
        "done" -> Json.fromInt(done.length),
        "fail" -> Json.fromInt(fail.length),
        "jobs" -> cb.jobs.map(cj =>
          Json.obj(
            show"job-${cj.job.index}" -> Json.fromString(cj.job.name),
            "took" -> Json.fromString(fmt.format(cj.took)),
            "kind" -> cj.job.kind.asJson,
            "done" -> Json.fromBoolean(cj.done)
          ))
          .asJson
      )
    }

sealed trait BatchResult[A] {
  def label: MetricLabel
  def spent: Duration
  def mode: BatchMode
  def batchId: UUID
  def jobs: List[A]
  def done: Boolean
  def completed: CompletedBatch
}

/** The aggregate result of a quasi-batch execution, where each job contributes a completion record and
  * outcome state.
  */
final case class QuasiBatch[A](
  label: MetricLabel,
  spent: Duration,
  mode: BatchMode,
  batchId: UUID,
  jobs: List[JobState[A]])
    extends BatchResult[JobState[A]] derives Functor {
  override def done: Boolean = jobs.forall(_.completed.done)
  override def completed: CompletedBatch = CompletedBatch(
    label = label,
    spent = spent,
    mode = mode,
    batchId = batchId,
    jobs = jobs.map(_.completed)
  )
}
object QuasiBatch:
  given [A: Encoder] => Encoder[QuasiBatch[A]] =
    Encoder.instance { qb =>
      val (done, fail) = qb.jobs.partition(_.completed.done)
      Json.obj(
        "batch" -> Json.fromString(qb.label.label),
        "batch_id" -> qb.batchId.asJson,
        "domain" -> Json.fromString(qb.label.domain.value),
        "mode" -> qb.mode.asJson,
        "spent" -> Json.fromString(fmt.format(qb.spent)),
        "done" -> Json.fromInt(done.length),
        "fail" -> Json.fromInt(fail.length),
        "jobs" -> qb.jobs.map { js =>
          val tag: String = if (js.done) "result" else "error"
          Json.obj(
            show"job-${js.completed.job.index}" -> Json.fromString(js.completed.job.name),
            "took" -> Json.fromString(fmt.format(js.completed.took)),
            tag -> js.result.asJson
          )
        }.asJson
      )
    }
end QuasiBatch

/** The aggregate result of a value-batch execution, where each job contributes a successful value and
  * completion metadata.
  */
final case class BatchValue[A](
  label: MetricLabel,
  spent: Duration,
  mode: BatchMode,
  batchId: UUID,
  jobs: List[JobValue[A]])
    extends BatchResult[JobValue[A]] derives Functor {
  override val done: Boolean = true
  override def completed: CompletedBatch =
    CompletedBatch(
      label = label,
      spent = spent,
      mode = mode,
      batchId = batchId,
      jobs = jobs.map(_.completed)
    )
}
object BatchValue:
  given [A: Encoder] => Encoder[BatchValue[A]] =
    Encoder.instance { bv =>
      Json.obj(
        "batch" -> Json.fromString(bv.label.label),
        "batch_id" -> bv.batchId.asJson,
        "domain" -> Json.fromString(bv.label.domain.value),
        "mode" -> bv.mode.asJson,
        "spent" -> Json.fromString(fmt.format(bv.spent)),
        "jobs" -> bv.jobs.map(js =>
          Json.obj(
            show"job-${js.completed.job.index}" -> Json.fromString(js.completed.job.name),
            "took" -> Json.fromString(fmt.format(js.completed.took)),
            "result" -> js.result.asJson
          ))
          .asJson
      )
    }
end BatchValue

/** The aggregate result of a monadic batch execution, including the recorded step history and final result.
  */
final case class MonadicBatch[A](
  label: MetricLabel,
  spent: Duration,
  batchId: UUID,
  jobs: List[CompletedJob],
  result: Either[Throwable, A])
    extends BatchResult[CompletedJob] derives Functor {
  override val mode: BatchMode = BatchMode.Monadic
  override def done: Boolean = result.isRight

  override def completed: CompletedBatch =
    CompletedBatch(
      label = label,
      spent = spent,
      mode = BatchMode.Monadic,
      batchId = batchId,
      jobs = jobs
    )
}
object MonadicBatch:
  given [A: Encoder] => Encoder[MonadicBatch[A]] =
    Encoder.instance { mb =>
      val tag: String = if (mb.done) "result" else "error"
      Json.obj(
        "batch" -> Json.fromString(mb.label.label),
        "batch_id" -> mb.batchId.asJson,
        "domain" -> Json.fromString(mb.label.domain.value),
        "mode" -> mb.mode.asJson,
        "spent" -> Json.fromString(fmt.format(mb.spent)),
        "jobs" -> mb.jobs.map(cj =>
          Json.obj(
            show"job-${cj.job.index}" -> Json.fromString(cj.job.name),
            "took" -> Json.fromString(fmt.format(cj.took))
          ))
          .asJson,
        tag -> mb.result.fold(StackTrace(_).asJson, _.asJson)
      )
    }
end MonadicBatch
