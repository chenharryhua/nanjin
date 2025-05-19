package com.github.chenharryhua.nanjin.guard.action
import cats.effect.std.Console
import cats.syntax.all.*
import cats.{Applicative, Show}
import com.github.chenharryhua.nanjin.guard.config.MetricLabel
import com.github.chenharryhua.nanjin.guard.service.Agent
import com.github.chenharryhua.nanjin.guard.translator.{decimalFormatter, durationFormatter}
import io.circe.syntax.EncoderOps
import io.circe.{Decoder, Encoder, Json}
import org.apache.commons.lang3.exception.ExceptionUtils
import squants.Dimensionless
import squants.information.{DataRate, Information}
import squants.time.{Frequency, Nanoseconds}

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

final class TraceJob[F[_], A] private (
  private[action] val completed: (JobOutcome, A) => F[Unit],
  private[action] val errored: (JobOutcome, Throwable) => F[Unit],
  private[action] val canceled: BatchJobID => F[Unit],
  private[action] val kickoff: BatchJobID => F[Unit]
) {
  private def copy(
    completed: (JobOutcome, A) => F[Unit] = this.completed,
    errored: (JobOutcome, Throwable) => F[Unit] = this.errored,
    canceled: BatchJobID => F[Unit] = this.canceled,
    kickoff: BatchJobID => F[Unit] = this.kickoff): TraceJob[F, A] =
    new TraceJob[F, A](completed, errored, canceled, kickoff)

  def contramap[B](f: B => A): TraceJob[F, B] =
    new TraceJob[F, B](
      completed = (job, b) => completed(job, f(b)),
      errored,
      canceled,
      kickoff
    )

  def onComplete(f: (JobOutcome, A) => F[Unit]): TraceJob[F, A]      = copy(completed = f)
  def onError(f: (JobOutcome, Throwable) => F[Unit]): TraceJob[F, A] = copy(errored = f)
  def onCancel(f: BatchJobID => F[Unit]): TraceJob[F, A]             = copy(canceled = f)
  def onKickoff(f: BatchJobID => F[Unit]): TraceJob[F, A]            = copy(kickoff = f)
}

object TraceJob {
  private val OUTCOME: String = "outcome"
  private val COUNT: String   = "count"

  def noop[F[_], A](implicit F: Applicative[F]): TraceJob[F, A] =
    new TraceJob(
      completed = (_, _) => F.unit,
      errored = (_, _) => F.unit,
      canceled = _ => F.unit,
      kickoff = _ => F.unit)

  def generic[F[_]: Console, A](agent: Agent[F]): TraceJob[F, A] =
    new TraceJob[F, A](
      completed = { (job, _) =>
        if (job.done)
          agent.herald.done(job)
        else
          agent.herald.error(job)
      },
      errored = (job, ex) => agent.herald.error(ex)(job),
      canceled = job => agent.console.warn(Json.obj("canceled" -> job.asJson)),
      kickoff = job => agent.console.debug(Json.obj("kickoff" -> job.asJson))
    )

  def json[F[_]: Console](agent: Agent[F]): TraceJob[F, Json] =
    generic[F, Json](agent).onComplete { case (job, json) =>
      val jo = Json.obj(OUTCOME -> json).deepMerge(job.asJson)
      if (job.done)
        agent.herald.done(jo)
      else
        agent.herald.error(jo)
    }

  def dataRate[F[_]: Console](agent: Agent[F]): TraceJob[F, Information] =
    generic[F, Information](agent).onComplete { case (job, number) =>
      val count: String = s"${decimalFormatter.format(number.value.toLong)} ${number.unit.symbol}"

      val speed: DataRate   = number / Nanoseconds(job.took.toNanos)
      val formatted: String = s"${decimalFormatter.format(speed.value.toLong)} ${speed.unit.symbol}"

      val jo: Json =
        Json
          .obj(
            "data-rate" ->
              Json.obj(COUNT -> Json.fromString(count), "speed" -> Json.fromString(formatted)))
          .deepMerge(job.asJson)

      if (job.done)
        agent.herald.done(jo)
      else
        agent.herald.error(jo)
    }

  def scalarRate[F[_]: Console](agent: Agent[F]): TraceJob[F, Dimensionless] =
    generic[F, Dimensionless](agent).onComplete { case (job, number) =>
      val count: String   = s"${decimalFormatter.format(number.value.toLong)} ${number.unit.symbol}"
      val rate: Frequency = number / Nanoseconds(job.took.toNanos)
      val ratio: Double   = number.value / number.toEach
      val formatted: String =
        s"${decimalFormatter.format((rate.toHertz * ratio).toLong)} ${number.unit.symbol}/s"

      val jo: Json =
        Json
          .obj(
            "scalar-rate" ->
              Json.obj(COUNT -> Json.fromString(count), "rate" -> Json.fromString(formatted)))
          .deepMerge(job.asJson)

      if (job.done)
        agent.herald.done(jo)
      else
        agent.herald.error(jo)
    }
}

final case class MeasuredValue[A](batch: MeasuredBatch, value: A)
object MeasuredValue {
  implicit def encoderMeasuredValue[A: Encoder]: Encoder[MeasuredValue[A]] =
    (a: MeasuredValue[A]) => Json.obj("batch" -> a.batch.asJson, "value" -> a.value.asJson)
}
