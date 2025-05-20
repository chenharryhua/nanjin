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

sealed trait BatchKind
object BatchKind {
  case object Quasi extends BatchKind
  case object Value extends BatchKind
  implicit val showBatchKind: Show[BatchKind] = {
    case Quasi => "quasi"
    case Value => "value"
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

/** @param name
  *   job name
  * @param index
  *   one based index
  */
final case class BatchJob(name: String, index: Int)
final case class MeasuredJob(job: BatchJob, took: Duration, done: Boolean)

final case class MeasuredBatch(
  label: MetricLabel,
  spent: Duration,
  mode: BatchMode,
  kind: BatchKind,
  jobs: List[MeasuredJob])
object MeasuredBatch {
  implicit val encoderBatchMeasurement: Encoder[MeasuredBatch] = { (br: MeasuredBatch) =>
    val (done, fail) = br.jobs.partition(_.done)
    Json.obj(
      "batch" -> Json.fromString(br.label.label),
      "domain" -> Json.fromString(br.label.domain.value),
      "mode" -> Json.fromString(br.mode.show),
      "kind" -> Json.fromString(br.kind.show),
      "spent" -> Json.fromString(durationFormatter.format(br.spent)),
      "done" -> Json.fromInt(done.length),
      "fail" -> Json.fromInt(fail.length),
      "measurements" -> br.jobs
        .map(jr =>
          Json.obj(
            show"job-${jr.job.index}" -> Json.fromString(jr.job.name),
            "took" -> Json.fromString(durationFormatter.format(jr.took)),
            "done" -> Json.fromBoolean(jr.done)
          ))
        .asJson
    )
  }
}

/*
 * for Job life-cycle handler
 */

final case class BatchJobID(job: BatchJob, label: MetricLabel, mode: BatchMode, kind: BatchKind)
object BatchJobID {
  implicit val encoderBatchJobID: Encoder[BatchJobID] =
    (a: BatchJobID) =>
      Json.obj(
        show"job-${a.job.index}" -> Json.fromString(a.job.name),
        "batch" -> Json.fromString(a.label.label),
        "domain" -> Json.fromString(a.label.domain.value),
        "mode" -> Json.fromString(a.mode.show),
        "kind" -> Json.fromString(a.kind.show)
      )
}

final case class JobOutcome(job: BatchJobID, took: Duration, done: Boolean)
object JobOutcome {
  implicit val encoderJobOutcome: Encoder[JobOutcome] =
    (a: JobOutcome) =>
      Json
        .obj("took" -> Json.fromString(durationFormatter.format(a.took)), "done" -> Json.fromBoolean(a.done))
        .deepMerge(a.job.asJson)
}

final case class MeasuredValue[A](batch: MeasuredBatch, value: A)
object MeasuredValue {
  implicit def encoderMeasuredValue[A: Encoder]: Encoder[MeasuredValue[A]] =
    (a: MeasuredValue[A]) => Json.obj("batch" -> a.batch.asJson, "value" -> a.value.asJson)
}
