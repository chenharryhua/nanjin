package com.github.chenharryhua.nanjin.guard.action
import cats.Show
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.config.MetricLabel
import com.github.chenharryhua.nanjin.guard.translator.fmt
import io.circe.syntax.EncoderOps
import io.circe.{Encoder, Json}

import java.time.Duration

sealed trait BatchKind
object BatchKind {
  case object Quasi extends BatchKind
  case object Batch extends BatchKind
  implicit val showBatchKind: Show[BatchKind] = {
    case Quasi => "quasi"
    case Batch => "batch"
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

final case class BatchJobName(value: String) extends AnyVal
object BatchJobName {
  implicit val showBatchJobName: Show[BatchJobName]       = _.value
  implicit val encoderBatchJobName: Encoder[BatchJobName] = Encoder.encodeString.contramap(_.value)
}

final case class BatchJob(kind: BatchKind, mode: BatchMode, name: Option[BatchJobName], index: Int, jobs: Int)
object BatchJob {
  implicit val encoderBatchJob: Encoder[BatchJob] = (a: BatchJob) =>
    Json.obj(
      show"${a.kind}" -> Json
        .obj(
          "name" -> a.name.asJson,
          "mode" -> a.mode.asJson,
          "index" -> Json.fromInt(a.index + 1),
          "jobs" -> Json.fromInt(a.jobs))
        .dropNullValues)
}

final case class Detail(job: BatchJob, took: Duration, done: Boolean)
final case class QuasiResult(metricName: MetricLabel, spent: Duration, mode: BatchMode, details: List[Detail])
object QuasiResult {
  implicit val encoderQuasiResult: Encoder[QuasiResult] = { (a: QuasiResult) =>
    val (done, fail) = a.details.partition(_.done)
    Json.obj(
      "name" -> Json.fromString(a.metricName.label),
      "digest" -> Json.fromString(a.metricName.digest),
      "mode" -> a.mode.asJson,
      "spent" -> Json.fromString(fmt.format(a.spent)),
      "done" -> Json.fromInt(done.length),
      "fail" -> Json.fromInt(fail.length),
      "details" -> a.details
        .map(d =>
          Json
            .obj(
              "name" -> d.job.name.asJson,
              "index" -> Json.fromInt(d.job.index + 1),
              "took" -> Json.fromString(fmt.format(d.took)),
              "done" -> Json.fromBoolean(d.done))
            .dropNullValues)
        .asJson
    )
  }
}
