package com.github.chenharryhua.nanjin.guard.action
import cats.Show
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.config.MetricLabel
import com.github.chenharryhua.nanjin.guard.translator.durationFormatter
import io.circe.syntax.EncoderOps
import io.circe.{Encoder, Json}

import java.time.Duration

sealed trait BatchKind
object BatchKind {
  case object Quasi extends BatchKind
  case object Fully extends BatchKind
  implicit val showBatchKind: Show[BatchKind] = {
    case Quasi => "quasi"
    case Fully => "fully"
  }
}

sealed trait BatchMode
object BatchMode {
  final case class Parallel(parallelism: Int) extends BatchMode
  case object Sequential extends BatchMode

  implicit val showBatchMode: Show[BatchMode] = {
    case Parallel(parallelism) => s"parallel-$parallelism"
    case Sequential            => "sequential"
  }

  implicit val encoderBatchMode: Encoder[BatchMode] =
    (a: BatchMode) => Json.fromString(a.show)
}

/** @param name
  *   optional job name
  * @param index
  *   one based index
  */
final case class BatchJob(name: Option[String], index: Int)

final case class Detail(job: BatchJob, took: Duration, done: Boolean)
final case class QuasiResult(label: MetricLabel, spent: Duration, mode: BatchMode, details: List[Detail])
object QuasiResult {
  implicit val encoderQuasiResult: Encoder[QuasiResult] = { (a: QuasiResult) =>
    val (done, fail) = a.details.partition(_.done)
    Json.obj(
      "batch" -> Json.fromString(a.label.label),
      "mode" -> a.mode.asJson,
      "spent" -> Json.fromString(durationFormatter.format(a.spent)),
      "done" -> Json.fromInt(done.length),
      "fail" -> Json.fromInt(fail.length),
      "details" -> a.details
        .map(d =>
          Json
            .obj(
              "name" -> d.job.name.asJson,
              "index" -> Json.fromInt(d.job.index),
              "took" -> Json.fromString(durationFormatter.format(d.took)),
              "done" -> Json.fromBoolean(d.done)
            )
            .dropNullValues)
        .asJson
    )
  }
}
