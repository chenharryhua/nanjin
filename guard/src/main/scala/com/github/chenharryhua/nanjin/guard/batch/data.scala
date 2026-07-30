package com.github.chenharryhua.nanjin.guard.batch

import cats.derived.derived
import cats.syntax.bifunctor.toBifunctorOps
import cats.syntax.show.{showInterpolator, toShow}
import cats.{Functor, Show}
import com.github.chenharryhua.nanjin.common.DurationFormatter.defaultFormatter
import com.github.chenharryhua.nanjin.guard.event.MetricLabel
import io.circe.syntax.EncoderOps
import io.circe.{Decoder, Encoder, Json}
import org.apache.commons.lang3.exception.ExceptionUtils

import java.time.Duration
import java.util.UUID
import scala.util.Try
import scala.util.matching.Regex

/** Distinguishes the two batch execution shapes: quasi-batches expose per-job outcome state, while
  * value-batches carry the successful result values for each completed job.
  */
enum BatchKind derives Encoder, Show:
  case Quasi, Value

/** Describes how a batch is executed. */
enum BatchMode:
  case Parallel(parallelism: Int)
  case Sequential
  case Monadic

/** Converts batch modes to stable strings for telemetry and configuration, and parses them back. */
object BatchMode {
  given Show[BatchMode] = {
    case Parallel(parallelism) => s"parallel-$parallelism"
    case Sequential            => "sequential"
    case Monadic               => "monadic"
  }

  given Encoder[BatchMode] =
    Encoder.instance { (a: BatchMode) =>
      Json.fromString(a.show)
    }

  private val Pattern: Regex = raw"parallel-(\d+)".r

  given Decoder[BatchMode] = Decoder[String].emap {
    case "sequential" => Right(Sequential)
    case "monadic"    => Right(Monadic)
    case Pattern(par) =>
      Try(par.toInt).filter(_ > 0).map(Parallel(_)).toEither.leftMap(ExceptionUtils.getMessage)
    case oops => Left(s"Invalid batch mode: $oops")
  }
}

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
      "mode" -> Json.fromString(a.mode.show),
      "kind" -> Json.fromString(a.kind.show)
    )
  }
}

/** A completed job record that captures its identity, elapsed time, and whether it finished successfully. */
final case class CompletedJob(job: Job, took: Duration, done: Boolean)

private given [A: Encoder] => Encoder[Either[Throwable, A]] =
  Encoder.instance {
    case Left(value)  => Json.fromString(ExceptionUtils.getMessage(value))
    case Right(value) => value.asJson
  }

/** The recorded outcome of a single batch job, including the completed job summary and its result. */
final case class JobState[A](completed: CompletedJob, result: Either[Throwable, A]) derives Functor
object JobState:
  given [A: Encoder] => Encoder[JobState[A]] = Encoder.instance { a =>
    Json.obj(
      "took" -> Json.fromString(defaultFormatter.format(a.completed.took)),
      "result" -> a.result.asJson)
      .deepMerge(a.completed.job.asJson)
  }

/** A successful batch job value paired with the completion metadata for that job. */
final case class JobValue[A](completed: CompletedJob, result: A) derives Functor
object JobValue:
  given [A: Encoder] => Encoder[JobValue[A]] = Encoder.instance { a =>
    Json.obj(
      "took" -> Json.fromString(defaultFormatter.format(a.completed.took)),
      "result" -> a.result.asJson)
      .deepMerge(a.completed.job.asJson)
  }

final case class CompletedBatch(
  label: MetricLabel,
  spent: Duration,
  mode: BatchMode,
  batchId: UUID,
  jobs: List[CompletedJob])
object CompletedBatch:
  given Encoder[CompletedBatch] =
    Encoder.instance { cb =>
      val (done, fail) = cb.jobs.partition(_.done)
      Json.obj(
        "batch" -> Json.fromString(cb.label.label),
        "batch_id" -> cb.batchId.asJson,
        "domain" -> Json.fromString(cb.label.domain.value),
        "mode" -> Json.fromString(cb.mode.show),
        "spent" -> Json.fromString(defaultFormatter.format(cb.spent)),
        "done" -> Json.fromInt(done.length),
        "fail" -> Json.fromInt(fail.length),
        "jobs" -> cb.jobs.sortBy(_.job.index)
          .map(cj =>
            Json.obj(
              show"job-${cj.job.index}" -> Json.fromString(cj.job.name),
              "took" -> Json.fromString(defaultFormatter.format(cj.took)),
              "kind" -> cj.job.kind.asJson,
              "done" -> Json.fromBoolean(cj.done)
            ))
          .asJson
      )
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
    derives Functor {
  def done: Boolean = jobs.forall(_.completed.done)
  def completed: CompletedBatch = CompletedBatch(
    label = label,
    spent = spent,
    mode = mode,
    batchId = batchId,
    jobs = jobs.map(_.completed)
  )
}
object QuasiBatch {
  given [A: Encoder] => Encoder[QuasiBatch[A]] =
    Encoder.instance { qb =>
      val (done, fail) = qb.jobs.partition(_.completed.done)
      Json.obj(
        "batch" -> Json.fromString(qb.label.label),
        "batch_id" -> qb.batchId.asJson,
        "domain" -> Json.fromString(qb.label.domain.value),
        "mode" -> Json.fromString(qb.mode.show),
        "spent" -> Json.fromString(defaultFormatter.format(qb.spent)),
        "done" -> Json.fromInt(done.length),
        "fail" -> Json.fromInt(fail.length),
        "results" -> qb.jobs.sortBy(_.completed.job.index)
          .map(js =>
            Json.obj(
              show"job-${js.completed.job.index}" -> Json.fromString(js.completed.job.name),
              "took" -> Json.fromString(defaultFormatter.format(js.completed.took)),
              "result" -> js.result.asJson
            ))
          .asJson
      )
    }
}

/** The aggregate result of a value-batch execution, where each job contributes a successful value and
  * completion metadata.
  */
final case class BatchValue[A](
  label: MetricLabel,
  spent: Duration,
  mode: BatchMode,
  batchId: UUID,
  jobs: List[JobValue[A]])
    derives Functor {
  val done: Boolean = true
  def completed: CompletedBatch = CompletedBatch(
    label = label,
    spent = spent,
    mode = mode,
    batchId = batchId,
    jobs = jobs.map(_.completed)
  )
}
object BatchValue {
  given [A: Encoder] => Encoder[BatchValue[A]] =
    Encoder.instance { bv =>
      Json.obj(
        "batch" -> Json.fromString(bv.label.label),
        "batch_id" -> bv.batchId.asJson,
        "domain" -> Json.fromString(bv.label.domain.value),
        "mode" -> Json.fromString(bv.mode.show),
        "spent" -> Json.fromString(defaultFormatter.format(bv.spent)),
        "total" -> Json.fromInt(bv.jobs.length),
        "results" -> bv.jobs.sortBy(_.completed.job.index)
          .map(js =>
            Json.obj(
              show"job-${js.completed.job.index}" -> Json.fromString(js.completed.job.name),
              "took" -> Json.fromString(defaultFormatter.format(js.completed.took)),
              "result" -> js.result.asJson
            ))
          .asJson
      )
    }
}

/** The aggregate result of a monadic batch execution, including the recorded step history and final result.
  */
final case class MonadicBatch[A](
  label: MetricLabel,
  spent: Duration,
  batchId: UUID,
  jobs: List[CompletedJob],
  result: Either[Throwable, A])
    derives Functor {
  def done: Boolean = result.isRight

  def completed: CompletedBatch = CompletedBatch(
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
      Json.obj(
        "batch" -> Json.fromString(mb.label.label),
        "batch_id" -> mb.batchId.asJson,
        "domain" -> Json.fromString(mb.label.domain.value),
        "spent" -> Json.fromString(defaultFormatter.format(mb.spent)),
        "total" -> Json.fromInt(mb.jobs.length),
        "steps" -> mb.jobs.sortBy(_.job.index)
          .map(cj =>
            Json.obj(
              show"job-${cj.job.index}" -> Json.fromString(cj.job.name),
              "took" -> Json.fromString(defaultFormatter.format(cj.took))
            ))
          .asJson,
        "result" -> mb.result.asJson
      )
    }

/** Raised when a batch job completes, but the post-condition predicate rejects the value. */
final case class PostConditionUnsatisfied(job: Job)
    extends Exception(s"post-condition check failed after: ${job.asJson.noSpaces}")

final private[batch] case class JobNameIndex[F[_], A](name: String, index: Int, fa: F[A])
