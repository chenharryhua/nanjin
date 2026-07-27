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

final case class JobState(job: BatchJob, took: Duration, done: Boolean):
  val fail: Boolean = !done

object JobState:
  given Encoder[JobState] =
    (a: JobState) =>
      Json.obj("took" -> Json.fromString(defaultFormatter.format(a.took))).deepMerge(a.job.asJson)

final case class JobValue[A](state: JobState, value: A):
  def map[B](f: A => B): JobValue[B] = copy(value = f(value))

final case class JobError(state: JobState, cause: Throwable)

final case class BatchState(
  label: MetricLabel,
  spent: Duration,
  mode: BatchMode,
  batchId: UUID,
  jobs: List[JobState])
object BatchState {
  given Encoder[BatchState] = { (br: BatchState) =>
    val (done, fail) = br.jobs.partition(_.done)
    Json.obj(
      "batch" -> Json.fromString(br.label.label),
      "batch_id" -> br.batchId.asJson,
      "domain" -> Json.fromString(br.label.domain.value),
      "mode" -> Json.fromString(br.mode.show),
      "spent" -> Json.fromString(defaultFormatter.format(br.spent)),
      "done" -> Json.fromInt(done.length),
      "fail" -> Json.fromInt(fail.length),
      "results" -> br.jobs.sortBy(_.job.index)
        .map(js =>
          Json.obj(
            show"job-${js.job.index}" -> Json.fromString(js.job.name),
            "took" -> Json.fromString(defaultFormatter.format(js.took)),
            "kind" -> Json.fromString(js.job.kind.show),
            "done" -> Json.fromBoolean(js.done)
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
  jobs: List[JobValue[A]]) {
  def state: BatchState =
    BatchState(
      label = label,
      spent = spent,
      mode = mode,
      batchId = batchId,
      jobs = jobs.map(_.state))
}

object BatchValue:
  given [A: Encoder] => Encoder[BatchValue[A]] =
    (bv: BatchValue[A]) =>
      Json.obj(
        "batch" -> Json.fromString(bv.label.label),
        "batch_id" -> bv.batchId.asJson,
        "domain" -> Json.fromString(bv.label.domain.value),
        "mode" -> Json.fromString(bv.mode.show),
        "spent" -> Json.fromString(defaultFormatter.format(bv.spent)),
        "results" -> bv.jobs.sortBy(_.state.job.index)
          .map(jv =>
            Json.obj(
              show"job-${jv.state.job.index}" -> Json.fromString(jv.state.job.name),
              "took" -> Json.fromString(defaultFormatter.format(jv.state.took)),
              "kind" -> Json.fromString(jv.state.job.kind.show),
              "value" -> jv.value.asJson
            ))
          .asJson
      )

final case class MonadicValue[A](state: BatchState, value: A)
object MonadicValue:
  given [A: Encoder] => Encoder[MonadicValue[A]] =
    (a: MonadicValue[A]) => Json.obj("value" -> a.value.asJson).deepMerge(a.state.asJson)

final case class PostConditionUnsatisfied(job: BatchJob)
    extends Exception(s"post-condition check failed after: ${job.asJson.noSpaces}")

final private[batch] case class JobNameIndex[F[_], A](name: String, index: Int, fa: F[A])
