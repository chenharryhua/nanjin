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
    (a: BatchMode) => Json.fromString(a.show)

  private val Pattern: Regex = raw"parallel-(\d+)".r

  given Decoder[BatchMode] = Decoder[String].emap {
    case "sequential" => Right(Sequential)
    case "monadic"    => Right(Monadic)
    case Pattern(par) =>
      Try(par.toInt).filter(_ > 0).map(Parallel(_)).toEither.leftMap(ExceptionUtils.getMessage)
    case oops => Left(s"Invalid batch mode: $oops")
  }
}

final case class BatchJob(
  name: String,
  index: Int,
  label: MetricLabel,
  mode: BatchMode,
  kind: BatchKind,
  batchId: UUID):
  val batch: String = label.label
  val domain: String = label.domain.value
  def displayName: String = s"job-$index $name"
end BatchJob

object BatchJob {
  given Encoder[BatchJob] =
    (a: BatchJob) =>
      Json.obj(
        show"job-${a.index}" -> Json.fromString(a.name),
        "batch" -> Json.fromString(a.batch),
        "batch_id" -> a.batchId.asJson,
        "domain" -> Json.fromString(a.domain),
        "mode" -> Json.fromString(a.mode.show),
        "kind" -> Json.fromString(a.kind.show)
      )
}

final case class JobResultState(job: BatchJob, took: Duration, done: Boolean):
  val fail: Boolean = !done

object JobResultState:
  given Encoder[JobResultState] =
    (a: JobResultState) =>
      Json.obj("took" -> Json.fromString(defaultFormatter.format(a.took))).deepMerge(a.job.asJson)

final case class JobResultValue[A](resultState: JobResultState, value: A):
  def map[B](f: A => B): JobResultValue[B] = copy(value = f(value))

final case class JobResultError(resultState: JobResultState, error: Throwable)

final case class BatchResultState(
  label: MetricLabel,
  spent: Duration,
  mode: BatchMode,
  batchId: UUID,
  jobs: List[JobResultState])
object BatchResultState {
  given Encoder[BatchResultState] = { (br: BatchResultState) =>
    val (done, fail) = br.jobs.partition(_.done)
    Json.obj(
      "batch" -> Json.fromString(br.label.label),
      "batch_id" -> br.batchId.asJson,
      "domain" -> Json.fromString(br.label.domain.value),
      "mode" -> Json.fromString(br.mode.show),
      "spent" -> Json.fromString(defaultFormatter.format(br.spent)),
      "done" -> Json.fromInt(done.length),
      "fail" -> Json.fromInt(fail.length),
      "results" -> br.jobs
        .map(jr =>
          Json.obj(
            show"job-${jr.job.index}" -> Json.fromString(jr.job.name),
            "kind" -> Json.fromString(jr.job.kind.show),
            "took" -> Json.fromString(defaultFormatter.format(jr.took)),
            "done" -> Json.fromBoolean(jr.done)
          ))
        .asJson
    )
  }
}

final case class BatchResultValue[A](resultState: BatchResultState, value: A):
  def map[B](f: A => B): BatchResultValue[B] = copy(value = f(value))

object BatchResultValue:
  given [A: Encoder] => Encoder[BatchResultValue[A]] =
    (a: BatchResultValue[A]) => Json.obj("batch" -> a.resultState.asJson, "value" -> a.value.asJson)

final case class PostConditionUnsatisfied(job: BatchJob)
    extends Exception(s"post-condition check failed after: ${job.asJson.noSpaces}")

final private[batch] case class JobNameIndex[F[_], A](name: String, index: Int, fa: F[A])
