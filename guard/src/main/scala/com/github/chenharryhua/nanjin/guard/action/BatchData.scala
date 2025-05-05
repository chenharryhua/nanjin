package com.github.chenharryhua.nanjin.guard.action
import cats.syntax.all.*
import cats.{Applicative, Show}
import com.github.chenharryhua.nanjin.guard.config.MetricLabel
import com.github.chenharryhua.nanjin.guard.translator.durationFormatter
import io.circe.generic.JsonCodec
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
@JsonCodec
final case class BatchJob(name: Option[String], index: Int)
@JsonCodec
final case class Detail(job: BatchJob, took: Duration, done: Boolean)

final case class BatchResult(label: MetricLabel, spent: Duration, mode: BatchMode, details: List[Detail])
object BatchResult {
  implicit val encoderBatchResult: Encoder[BatchResult] = { (br: BatchResult) =>
    val (done, fail) = br.details.partition(_.done)
    Json.obj(
      "batch" -> Json.fromString(br.label.label),
      "mode" -> br.mode.asJson,
      "spent" -> Json.fromString(durationFormatter.format(br.spent)),
      DONE -> Json.fromInt(done.length),
      FAIL -> Json.fromInt(fail.length),
      "details" -> br.details
        .map(detail =>
          Json
            .obj(
              NAME -> detail.job.name.asJson,
              INDEX -> Json.fromInt(detail.job.index),
              TOOK -> Json.fromString(durationFormatter.format(detail.took)),
              DONE -> Json.fromBoolean(detail.done)
            )
            .dropNullValues)
        .asJson
    )
  }
}

final case class JobTenure(job: BatchJob, took: Duration, done: Boolean)
object JobTenure {
  implicit val encoderJobTenure: Encoder[JobTenure] = { (jt: JobTenure) =>
    Json.obj(
      NAME -> jt.job.name.asJson,
      INDEX -> Json.fromInt(jt.job.index),
      TOOK -> Json.fromString(durationFormatter.format(jt.took)),
      DONE -> Json.fromBoolean(jt.done)
    )
  }
}

final case class HandleJobOutcome[F[_], A](
  completed: (JobTenure, A) => F[Unit],
  errored: (JobTenure, Throwable) => F[Unit],
  canceled: BatchJob => F[Unit]
)

object HandleJobOutcome {
  def noop[F[_], A](implicit F: Applicative[F]): HandleJobOutcome[F, A] =
    HandleJobOutcome((_, _) => F.unit, (_, _) => F.unit, _ => F.unit)
}
