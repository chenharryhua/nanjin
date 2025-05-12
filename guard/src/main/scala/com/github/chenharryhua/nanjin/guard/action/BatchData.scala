package com.github.chenharryhua.nanjin.guard.action
import cats.syntax.all.*
import cats.{Applicative, Show}
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

final case class BatchResult(
  label: MetricLabel,
  spent: Duration,
  mode: BatchMode,
  kind: BatchKind,
  results: List[JobResult])
object BatchResult {
  implicit val encoderBatchResult: Encoder[BatchResult] = { (br: BatchResult) =>
    val (done, fail) = br.results.partition(_.done)
    Json.obj(
      "domain" -> Json.fromString(br.label.domain.value),
      "batch" -> Json.fromString(br.label.label),
      "mode" -> Json.fromString(br.mode.show),
      "kind" -> Json.fromString(br.kind.show),
      "spent" -> Json.fromString(durationFormatter.format(br.spent)),
      "done" -> Json.fromInt(done.length),
      "fail" -> Json.fromInt(fail.length),
      "results" -> br.results
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
