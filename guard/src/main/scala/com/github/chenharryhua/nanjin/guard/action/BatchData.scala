package com.github.chenharryhua.nanjin.guard.action
import cats.syntax.all.*
import cats.{Applicative, Show}
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
object BatchJob {
  implicit val encoderBatchJob: Encoder[BatchJob] =
    (bj: BatchJob) =>
      bj.name.fold(Json.obj("index" -> Json.fromInt(bj.index)))(name =>
        Json.obj("job_name" -> Json.fromString(name), "index" -> Json.fromInt(bj.index)))
}

final case class JobDetail(job: BatchJob, took: Duration, done: Boolean)
object JobDetail {
  implicit val encoderJobDetail: Encoder[JobDetail] =
    (detail: JobDetail) =>
      Json
        .obj(
          "took" -> Json.fromString(durationFormatter.format(detail.took)),
          "done" -> Json.fromBoolean(detail.done)
        )
        .deepMerge(detail.job.asJson)
}

final case class BatchResult(label: MetricLabel, spent: Duration, mode: BatchMode, details: List[JobDetail])
object BatchResult {
  implicit val encoderBatchResult: Encoder[BatchResult] = { (br: BatchResult) =>
    val (done, fail) = br.details.partition(_.done)
    Json.obj(
      "batch" -> Json.fromString(br.label.label),
      "mode" -> br.mode.asJson,
      "spent" -> Json.fromString(durationFormatter.format(br.spent)),
      "done" -> Json.fromInt(done.length),
      "fail" -> Json.fromInt(fail.length),
      "details" -> br.details.asJson
    )
  }
}

final case class HandleJobOutcome[F[_], A](
  completed: (JobDetail, A) => F[Unit],
  errored: (JobDetail, Throwable) => F[Unit],
  canceled: BatchJob => F[Unit]
)

object HandleJobOutcome {
  def noop[F[_], A](implicit F: Applicative[F]): HandleJobOutcome[F, A] =
    HandleJobOutcome((_, _) => F.unit, (_, _) => F.unit, _ => F.unit)
}
