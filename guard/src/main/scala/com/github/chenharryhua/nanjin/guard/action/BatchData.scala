package com.github.chenharryhua.nanjin.guard.action
import cats.Show
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.config.MetricLabel
import com.github.chenharryhua.nanjin.guard.translator.durationFormatter
import io.circe.syntax.EncoderOps
import io.circe.{Decoder, Encoder, Json}
import org.apache.commons.lang3.exception.ExceptionUtils

import java.time.Duration
import scala.util.Try
import scala.util.matching.Regex

sealed trait JobKind
object JobKind {
  case object Quasi extends JobKind
  case object Value extends JobKind
  implicit val showBatchKind: Show[JobKind] = {
    case Quasi => "quasi" // tolerable - exception will be ignored
    case Value => "value" // intolerable - exception will be propagated
  }
}

sealed trait BatchMode
object BatchMode {
  final case class Parallel(parallelism: Int) extends BatchMode
  case object Sequential extends BatchMode
  case object Monadic extends BatchMode

  implicit val showBatchMode: Show[BatchMode] = {
    case Parallel(parallelism) => s"parallel-$parallelism"
    case Sequential            => "sequential"
    case Monadic               => "monadic"
  }

  implicit val encoderBatchMode: Encoder[BatchMode] =
    (a: BatchMode) => Json.fromString(a.show)

  private val Pattern: Regex = raw"parallel-(\d+)".r

  implicit val decodeBatchMode: Decoder[BatchMode] = Decoder[String].emap {
    case "sequential" => Right(Sequential)
    case "monadic"    => Right(Monadic)
    case Pattern(par) => Try(Parallel(par.toInt)).toEither.leftMap(ExceptionUtils.getMessage)
    case oops         => Left(s"$oops is unable to be decoded to batch mode")
  }
}

final case class BatchJob(name: String, index: Int, label: MetricLabel, mode: BatchMode, kind: JobKind) {
  val batch: String       = label.label
  val domain: String      = label.domain.value
  val indexedName: String = s"job-$index ($name)"
}
object BatchJob {
  implicit val encoderBatchJob: Encoder[BatchJob] =
    (a: BatchJob) =>
      Json.obj(
        show"job-${a.index}" -> Json.fromString(a.name),
        "batch" -> Json.fromString(a.batch),
        "domain" -> Json.fromString(a.domain),
        "mode" -> Json.fromString(a.mode.show),
        "kind" -> Json.fromString(a.kind.show)
      )
}

final case class JobResultState(job: BatchJob, took: Duration, done: Boolean)
object JobResultState {
  implicit val encoderJobResultState: Encoder[JobResultState] =
    (a: JobResultState) =>
      Json
        .obj("took" -> Json.fromString(durationFormatter.format(a.took)), "done" -> Json.fromBoolean(a.done))
        .deepMerge(a.job.asJson)
}

final case class JobResultValue[A](resultState: JobResultState, value: A) {
  def map[B](f: A => B): JobResultValue[B] = copy(value = f(value))
}

final case class JobResultError(resultState: JobResultState, error: Throwable)

final case class BatchResultState(
  label: MetricLabel,
  spent: Duration,
  mode: BatchMode,
  jobs: List[JobResultState])
object BatchResultState {
  implicit val encoderBatchResultState: Encoder[BatchResultState] = { (br: BatchResultState) =>
    val (done, fail) = br.jobs.partition(_.done)
    Json.obj(
      "batch" -> Json.fromString(br.label.label),
      "domain" -> Json.fromString(br.label.domain.value),
      "mode" -> Json.fromString(br.mode.show),
      "spent" -> Json.fromString(durationFormatter.format(br.spent)),
      "done" -> Json.fromInt(done.length),
      "fail" -> Json.fromInt(fail.length),
      "results" -> br.jobs
        .map(jr =>
          Json.obj(
            show"job-${jr.job.index}" -> Json.fromString(jr.job.name),
            "kind" -> Json.fromString(jr.job.kind.show),
            "took" -> Json.fromString(durationFormatter.format(jr.took)),
            "done" -> Json.fromBoolean(jr.done)
          ))
        .asJson
    )
  }
}

final case class BatchResultValue[A](resultState: BatchResultState, value: A) {
  def map[B](f: A => B): BatchResultValue[B] = copy(value = f(value))
}
object BatchResultValue {
  implicit def encoderBatchResultValue[A: Encoder]: Encoder[BatchResultValue[A]] =
    (a: BatchResultValue[A]) => Json.obj("batch" -> a.resultState.asJson, "value" -> a.value.asJson)
}

final case class PostConditionUnsatisfied(job: BatchJob) extends Exception(
      s"${job.indexedName} of batch(${job.batch}) in domain(${job.domain}) run to the end without exception but failed post-condition check")
