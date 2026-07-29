package com.github.chenharryhua.nanjin.guard.batch

import cats.Show
import cats.derived.derived
import cats.syntax.bifunctor.toBifunctorOps
import cats.syntax.show.{showInterpolator, toShow}
import com.github.chenharryhua.nanjin.common.DurationFormatter.defaultFormatter
import com.github.chenharryhua.nanjin.guard.event.MetricLabel
import io.circe.syntax.EncoderOps
import io.circe.{Decoder, Encoder, Json}
import org.apache.commons.lang3.exception.ExceptionUtils

import java.time.Duration
import java.util.UUID
import scala.util.Try
import scala.util.matching.Regex

enum BatchKind derives Encoder, Show:
  case Quasi, Value

enum BatchMode:
  case Parallel(parallelism: Int)
  case Sequential
  case Monadic

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

/*
 * Job
 */

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

final case class CompletedJob(job: Job, took: Duration, done: Boolean)

private given [A: Encoder] => Encoder[Either[Throwable, A]] =
  Encoder.instance {
    case Left(value)  => Json.fromString(ExceptionUtils.getMessage(value))
    case Right(value) => value.asJson
  }

/*
 * Job State and Value
 */
final case class JobState[A](completed: CompletedJob, result: Either[Throwable, A]):
  def map[B](f: A => B): JobState[B] = copy(result = result.map(f))
end JobState
object JobState:
  given [A: Encoder] => Encoder[JobState[A]] = Encoder.instance { a =>
    Json.obj(
      "took" -> Json.fromString(defaultFormatter.format(a.completed.took)),
      "result" -> a.result.asJson)
      .deepMerge(a.completed.job.asJson)
  }

final case class JobValue[A](completed: CompletedJob, result: A):
  val state: JobState[A] = JobState(completed, Right(result))
end JobValue
object JobValue:
  given [A: Encoder] => Encoder[JobValue[A]] = Encoder.instance { a =>
    Json.obj(
      "took" -> Json.fromString(defaultFormatter.format(a.completed.took)),
      "result" -> a.result.asJson)
      .deepMerge(a.completed.job.asJson)
  }

/*
 * Batch
 */

final case class BatchState[A](
  label: MetricLabel,
  spent: Duration,
  mode: BatchMode,
  batchId: UUID,
  jobs: List[JobState[A]]) {
  def done: Boolean = jobs.forall(_.completed.done)
}
object BatchState {
  given [A: Encoder] => Encoder[BatchState[A]] =
    Encoder.instance { bs =>
      val (done, fail) = bs.jobs.partition(_.completed.done)
      Json.obj(
        "batch" -> Json.fromString(bs.label.label),
        "batch_id" -> bs.batchId.asJson,
        "domain" -> Json.fromString(bs.label.domain.value),
        "mode" -> Json.fromString(bs.mode.show),
        "spent" -> Json.fromString(defaultFormatter.format(bs.spent)),
        "done" -> Json.fromInt(done.length),
        "fail" -> Json.fromInt(fail.length),
        "results" -> bs.jobs.sortBy(_.completed.job.index)
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

final case class BatchValue[A](
  label: MetricLabel,
  spent: Duration,
  mode: BatchMode,
  batchId: UUID,
  jobs: List[JobValue[A]])
object BatchValue {
  given [A: Encoder] => Encoder[BatchValue[A]] =
    Encoder.instance { bv =>
      Json.obj(
        "batch" -> Json.fromString(bv.label.label),
        "batch_id" -> bv.batchId.asJson,
        "domain" -> Json.fromString(bv.label.domain.value),
        "mode" -> Json.fromString(bv.mode.show),
        "spent" -> Json.fromString(defaultFormatter.format(bv.spent)),
        "done" -> Json.fromInt(bv.jobs.length),
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

final case class MonadicValue[A](
  label: MetricLabel,
  spent: Duration,
  batchId: UUID,
  jobs: List[CompletedJob],
  result: A)
object MonadicValue:
  given [A: Encoder] => Encoder[MonadicValue[A]] =
    Encoder.instance { mv =>
      Json.obj(
        "batch" -> Json.fromString(mv.label.label),
        "batch_id" -> mv.batchId.asJson,
        "domain" -> Json.fromString(mv.label.domain.value),
        "spent" -> Json.fromString(defaultFormatter.format(mv.spent)),
        "done" -> Json.fromInt(mv.jobs.length),
        "sequence" -> mv.jobs.sortBy(_.job.index)
          .map(cj =>
            Json.obj(
              show"job-${cj.job.index}" -> Json.fromString(cj.job.name),
              "took" -> Json.fromString(defaultFormatter.format(cj.took))
            ))
          .asJson,
        "result" -> mv.result.asJson
      )
    }

final case class PostConditionUnsatisfied(job: Job)
    extends Exception(s"post-condition check failed after: ${job.asJson.noSpaces}")

final private[batch] case class JobNameIndex[F[_], A](name: String, index: Int, fa: F[A])
