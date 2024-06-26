package com.github.chenharryhua.nanjin.guard.action
import cats.Show
import cats.effect.kernel.Unique
import cats.syntax.all.*
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
    Json
      .obj(
        show"${a.kind}" -> Json.obj(
          "mode" -> a.mode.asJson,
          "name" -> a.name.asJson,
          "index" -> (a.index + 1).asJson,
          "jobs" -> a.jobs.asJson))
      .deepDropNullValues
}

final case class Detail(job: BatchJob, took: Duration, done: Boolean)
final case class QuasiResult(token: Unique.Token, mode: BatchMode, spent: Duration, details: List[Detail])
object QuasiResult {
  val BatchIdTag: String = "id"
  implicit val encoderQuasiResult: Encoder[QuasiResult] =
    (a: QuasiResult) =>
      Json.obj(
        BatchIdTag -> a.token.hash.asJson,
        "mode" -> a.mode.asJson,
        "spent" -> fmt.format(a.spent).asJson,
        "details" -> a.details
          .map(d =>
            Json
              .obj(
                "name" -> d.job.name.asJson,
                "index" -> (d.job.index + 1).asJson,
                "took" -> fmt.format(d.took).asJson,
                "done" -> d.done.asJson)
              .dropNullValues)
          .asJson
      )
}
