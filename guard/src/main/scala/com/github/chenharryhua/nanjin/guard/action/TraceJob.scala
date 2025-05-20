package com.github.chenharryhua.nanjin.guard.action

import cats.{Applicative, Endo}
import com.github.chenharryhua.nanjin.guard.service.Agent
import com.github.chenharryhua.nanjin.guard.translator.decimalFormatter
import io.circe.Json
import io.circe.syntax.EncoderOps
import squants.Dimensionless
import squants.information.{DataRate, Information}
import squants.time.{Frequency, Nanoseconds}

final class TraceJob[F[_], A] private (
  translate: (JobOutcome, A) => Json,
  onKickoff: Json => F[Unit],
  onFailure: Json => F[Unit],
  onSuccess: Json => F[Unit],
  onCancel: Json => F[Unit],
  onError: (Json, Throwable) => F[Unit]
) {
  private[action] def kickoff(bj: BatchJobID): F[Unit] =
    onKickoff(Json.obj("kickoff" -> bj.asJson))

  private[action] def canceled(bj: BatchJobID): F[Unit] =
    onCancel(Json.obj("canceled" -> bj.asJson))

  private[action] def completed(jo: JobOutcome, a: A): F[Unit] = {
    val json = Json.obj("outcome" -> translate(jo, a)).deepMerge(jo.asJson)
    if (jo.done) onSuccess(json) else onFailure(json)
  }

  private[action] def errored(jo: JobOutcome, ex: Throwable): F[Unit] =
    onError(jo.asJson, ex)

  def contramap[B](f: B => A): TraceJob[F, B] =
    new TraceJob[F, B](
      translate = (jo, b) => translate(jo, f(b)),
      onKickoff = onKickoff,
      onFailure = onFailure,
      onSuccess = onSuccess,
      onCancel = onCancel,
      onError = onError
    )
}

object TraceJob {

  def noop[F[_], A](implicit F: Applicative[F]): TraceJob[F, A] =
    new TraceJob(
      translate = (_, _) => Json.Null,
      onKickoff = _ => F.unit,
      onFailure = _ => F.unit,
      onSuccess = _ => F.unit,
      onCancel = _ => F.unit,
      onError = (_, _) => F.unit
    )

  final class Builder[F[_]] private (agent: Agent[F])(
    private[TraceJob] val onKickoff: Json => F[Unit],
    private[TraceJob] val onSuccess: Json => F[Unit],
    private[TraceJob] val onFailure: Json => F[Unit],
    private[TraceJob] val onCancel: Json => F[Unit],
    private[TraceJob] val onError: (Json, Throwable) => F[Unit]
  ) {
    private def copy(
      onKickoff: Json => F[Unit] = this.onKickoff,
      onSuccess: Json => F[Unit] = this.onSuccess,
      onFailure: Json => F[Unit] = this.onFailure
    ): Builder[F] =
      new Builder[F](agent)(
        onKickoff = onKickoff,
        onSuccess = onSuccess,
        onFailure = onFailure,
        onCancel = onCancel,
        onError = onError)

    object anchor {
      object herald {
        val error: Json => F[Unit] = agent.herald.error(_)
        val warn: Json => F[Unit]  = agent.herald.warn(_)
        val done: Json => F[Unit]  = agent.herald.done(_)
        val info: Json => F[Unit]  = agent.herald.info(_)
      }
      object console {
        val warn: Json => F[Unit]  = agent.console.warn(_)
        val done: Json => F[Unit]  = agent.console.done(_)
        val info: Json => F[Unit]  = agent.console.info(_)
        val debug: Json => F[Unit] = agent.console.debug(_)
      }
      val void: Json => F[Unit] = agent.console.void(_)
    }

    def sendKickoffTo(f: anchor.type => Json => F[Unit]): Builder[F] =
      copy(onKickoff = f(anchor))

    def sendSuccessTo(f: anchor.type => Json => F[Unit]): Builder[F] =
      copy(onSuccess = f(anchor))

    def sendFailureTo(f: anchor.type => Json => F[Unit]): Builder[F] =
      copy(onFailure = f(anchor))

    private[TraceJob] def buildWith[A](translate: (JobOutcome, A) => Json): TraceJob[F, A] =
      new TraceJob[F, A](
        translate = translate,
        onKickoff = this.onKickoff,
        onFailure = this.onFailure,
        onSuccess = this.onSuccess,
        onCancel = this.onCancel,
        onError = this.onError
      )
  }

  private object Builder {
    def apply[F[_]](agent: Agent[F]) =
      new Builder[F](agent)(
        onKickoff = agent.console.debug(_),
        onSuccess = agent.herald.done(_),
        onFailure = agent.herald.error(_),
        onCancel = agent.console.warn(_),
        onError = (job, ex) => agent.herald.error(ex)(job)
      )
  }

  def generic[F[_], A](agent: Agent[F], f: Endo[Builder[F]] = identity[Builder[F]]): TraceJob[F, A] =
    f(Builder(agent)).buildWith[A]((_, _) => Json.Null)

  def json[F[_]](agent: Agent[F], f: Endo[Builder[F]] = identity[Builder[F]]): TraceJob[F, Json] =
    f(Builder(agent)).buildWith[Json]((_, json) => json)

  private val COUNT: String = "count"

  def dataRate[F[_]](
    agent: Agent[F],
    f: Endo[Builder[F]] = identity[Builder[F]]): TraceJob[F, Information] = {
    def translate(job: JobOutcome, number: Information): Json = {
      val count: String = s"${decimalFormatter.format(number.value.toLong)} ${number.unit.symbol}"

      val speed: DataRate   = number / Nanoseconds(job.took.toNanos)
      val formatted: String = s"${decimalFormatter.format(speed.value.toLong)} ${speed.unit.symbol}"

      Json.obj(COUNT -> Json.fromString(count), "speed" -> Json.fromString(formatted))
    }

    f(Builder(agent)).buildWith[Information](translate)
  }

  def scalarRate[F[_]](
    agent: Agent[F],
    f: Endo[Builder[F]] = identity[Builder[F]]): TraceJob[F, Dimensionless] = {
    def translate(job: JobOutcome, number: Dimensionless): Json = {
      val count: String   = s"${decimalFormatter.format(number.value.toLong)} ${number.unit.symbol}"
      val rate: Frequency = number / Nanoseconds(job.took.toNanos)
      val ratio: Double   = number.value / number.toEach
      val formatted: String =
        s"${decimalFormatter.format((rate.toHertz * ratio).toLong)} ${number.unit.symbol}/s"

      Json.obj(COUNT -> Json.fromString(count), "rate" -> Json.fromString(formatted))
    }

    f(Builder(agent)).buildWith[Dimensionless](translate)
  }
}
