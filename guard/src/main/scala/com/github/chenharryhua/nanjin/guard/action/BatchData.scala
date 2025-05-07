package com.github.chenharryhua.nanjin.guard.action
import cats.syntax.all.*
import cats.{Applicative, Show}
import com.github.chenharryhua.nanjin.guard.config.MetricLabel
import com.github.chenharryhua.nanjin.guard.translator.durationFormatter
import io.circe.generic.JsonCodec
import io.circe.syntax.EncoderOps
import io.circe.{Decoder, Encoder, Json}
import org.apache.commons.lang3.exception.ExceptionUtils

import java.time.Duration
import scala.util.Try
import scala.util.matching.Regex

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

  private val Pattern: Regex = raw"parallel-(\d+)".r

  implicit val decodeBatchMode: Decoder[BatchMode] = Decoder[String].emap {
    case "sequential" => Right(Sequential)
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
final case class JobResult(job: BatchJob, took: Duration, done: Boolean)

final case class BatchResult(label: MetricLabel, spent: Duration, mode: BatchMode, results: List[JobResult])
object BatchResult {
  implicit val encoderBatchResult: Encoder[BatchResult] = { (br: BatchResult) =>
    val (done, fail) = br.results.partition(_.done)
    Json.obj(
      "domain" -> Json.fromString(br.label.domain.value),
      "batch" -> Json.fromString(br.label.label),
      "mode" -> br.mode.asJson,
      "spent" -> Json.fromString(durationFormatter.format(br.spent)),
      "done" -> Json.fromInt(done.length),
      "fail" -> Json.fromInt(fail.length),
      "results" -> br.results
        .map(jr =>
          Json.obj(
            "job" -> Json.fromString(jr.job.name),
            "index" -> Json.fromInt(jr.job.index),
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

@JsonCodec
final case class BatchJobID private (name: String, index: Int, batch: String, mode: BatchMode, domain: String)
object BatchJobID {
  def apply(job: BatchJob, label: MetricLabel, mode: BatchMode): BatchJobID =
    BatchJobID(
      name = job.name,
      index = job.index,
      batch = label.label,
      mode = mode,
      domain = label.domain.value)
}
@JsonCodec
final case class JobOutcome(job: BatchJobID, took: String, done: Boolean)

final class HandleJobLifecycle[F[_], A] private (
  private[action] val completed: (JobOutcome, A) => F[Unit],
  private[action] val errored: (JobOutcome, Throwable) => F[Unit],
  private[action] val canceled: BatchJobID => F[Unit],
  private[action] val kickoff: BatchJobID => F[Unit]
) {
  private def copy(
    completed: (JobOutcome, A) => F[Unit] = this.completed,
    errored: (JobOutcome, Throwable) => F[Unit] = this.errored,
    canceled: BatchJobID => F[Unit] = this.canceled,
    kickoff: BatchJobID => F[Unit] = this.kickoff): HandleJobLifecycle[F, A] =
    new HandleJobLifecycle[F, A](completed, errored, canceled, kickoff)

  def onComplete(f: (JobOutcome, A) => F[Unit]): HandleJobLifecycle[F, A]      = copy(completed = f)
  def onError(f: (JobOutcome, Throwable) => F[Unit]): HandleJobLifecycle[F, A] = copy(errored = f)
  def onCancel(f: BatchJobID => F[Unit]): HandleJobLifecycle[F, A]             = copy(canceled = f)
  def onKickoff(f: BatchJobID => F[Unit]): HandleJobLifecycle[F, A]            = copy(kickoff = f)
}

object HandleJobLifecycle {
  def apply[F[_], A](implicit F: Applicative[F]): HandleJobLifecycle[F, A] =
    new HandleJobLifecycle(
      completed = (_, _) => F.unit,
      errored = (_, _) => F.unit,
      canceled = _ => F.unit,
      kickoff = _ => F.unit)
}
