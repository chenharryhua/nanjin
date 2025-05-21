package com.github.chenharryhua.nanjin.guard.action

import cats.{Applicative, Endo}
import com.github.chenharryhua.nanjin.guard.service.Agent
import io.circe.syntax.EncoderOps
import io.circe.{Encoder, Json}
import squants.Dimensionless
import squants.information.Information

sealed trait TraceJob[F[_], A] {
  private[action] def kickoff(bj: BatchJobID): F[Unit]
  private[action] def canceled(bj: BatchJobID): F[Unit]
  private[action] def completed(jo: JobOutcome, a: A): F[Unit]
  private[action] def errored(jo: JobOutcome, ex: Throwable): F[Unit]
  private[action] def predicate(a: A): Boolean
}

object TraceJob {

  def noop[F[_], A](implicit F: Applicative[F]): TraceJob[F, A] =
    new TraceJob[F, A] {
      override private[action] def errored(jo: JobOutcome, ex: Throwable): F[Unit] = F.unit
      override private[action] def completed(jo: JobOutcome, a: A): F[Unit]        = F.unit
      override private[action] def canceled(bj: BatchJobID): F[Unit]               = F.unit
      override private[action] def kickoff(bj: BatchJobID): F[Unit]                = F.unit

      override private[action] def predicate(a: A): Boolean = true
    }

  final class GenericTracer[F[_], A] private[TraceJob] (
    private[TraceJob] val _completed: (JobOutcome, A) => F[Unit],
    private[TraceJob] val _errored: (JobOutcome, Throwable) => F[Unit],
    private[TraceJob] val _canceled: BatchJobID => F[Unit],
    private[TraceJob] val _kickoff: BatchJobID => F[Unit],
    private[TraceJob] val _predicate: A => Boolean
  ) extends TraceJob[F, A] {
    override private[action] def kickoff(bj: BatchJobID): F[Unit]                = _kickoff(bj)
    override private[action] def canceled(bj: BatchJobID): F[Unit]               = _canceled(bj)
    override private[action] def completed(jo: JobOutcome, a: A): F[Unit]        = _completed(jo, a)
    override private[action] def errored(jo: JobOutcome, ex: Throwable): F[Unit] = _errored(jo, ex)
    override private[action] def predicate(a: A): Boolean                        = _predicate(a)

    private def copy(
      _completed: (JobOutcome, A) => F[Unit] = this._completed,
      _errored: (JobOutcome, Throwable) => F[Unit] = this._errored,
      _canceled: BatchJobID => F[Unit] = this._canceled,
      _kickoff: BatchJobID => F[Unit] = this._kickoff,
      _predicate: A => Boolean = this._predicate): GenericTracer[F, A] =
      new GenericTracer[F, A](_completed, _errored, _canceled, _kickoff, _predicate)

    def contramap[B](f: B => A): GenericTracer[F, B] =
      new GenericTracer[F, B](
        _completed = (job, b) => _completed(job, f(b)),
        _errored,
        _canceled,
        _kickoff,
        _predicate = b => _predicate(f(b))
      )

    def onComplete(f: (JobOutcome, A) => F[Unit]): GenericTracer[F, A]      = copy(_completed = f)
    def onError(f: (JobOutcome, Throwable) => F[Unit]): GenericTracer[F, A] = copy(_errored = f)
    def onCancel(f: BatchJobID => F[Unit]): GenericTracer[F, A]             = copy(_canceled = f)
    def onKickoff(f: BatchJobID => F[Unit]): GenericTracer[F, A]            = copy(_kickoff = f)
    def withPredicate(f: A => Boolean): GenericTracer[F, A]                 = copy(_predicate = f)
  }

  def generic[F[_], A](implicit F: Applicative[F]): GenericTracer[F, A] =
    new GenericTracer[F, A](
      _completed = (_, _) => F.unit,
      _errored = (_, _) => F.unit,
      _canceled = _ => F.unit,
      _kickoff = _ => F.unit,
      _predicate = _ => true
    )

  /*
   * Trace job using json format
   */

  final class JsonTracer[F[_], A] private[TraceJob] (
    _translate: (JobOutcome, A) => Json,
    _onKickoff: Json => F[Unit],
    _onFailure: Json => F[Unit],
    _onSuccess: Json => F[Unit],
    _onCancel: Json => F[Unit],
    _onError: (Json, Throwable) => F[Unit],
    _predicate: A => Boolean
  ) extends TraceJob[F, A] {
    override private[action] def kickoff(bj: BatchJobID): F[Unit] =
      _onKickoff(Json.obj("kickoff" -> bj.asJson))

    override private[action] def canceled(bj: BatchJobID): F[Unit] =
      _onCancel(Json.obj("canceled" -> bj.asJson))

    override private[action] def completed(jo: JobOutcome, a: A): F[Unit] = {
      val json = Json.obj("outcome" -> _translate(jo, a)).deepMerge(jo.asJson)
      if (jo.done) _onSuccess(json) else _onFailure(json)
    }

    override private[action] def errored(jo: JobOutcome, ex: Throwable): F[Unit] =
      _onError(jo.asJson, ex)

    override private[action] def predicate(a: A): Boolean = _predicate(a)

    def contramap[B](f: B => A): JsonTracer[F, B] =
      new JsonTracer[F, B](
        _translate = (jo, b) => this._translate(jo, f(b)),
        _onKickoff = this._onKickoff,
        _onFailure = this._onFailure,
        _onSuccess = this._onSuccess,
        _onCancel = this._onCancel,
        _onError = this._onError,
        _predicate = b => this._predicate(f(b))
      )

    def withPredicate(f: A => Boolean): JsonTracer[F, A] =
      new JsonTracer[F, A](
        _translate = this._translate,
        _onKickoff = this._onKickoff,
        _onFailure = this._onFailure,
        _onSuccess = this._onSuccess,
        _onCancel = this._onCancel,
        _onError = this._onError,
        _predicate = f
      )
  }

  final class Builder[F[_]] private (agent: Agent[F])(
    private[TraceJob] val _onKickoff: Json => F[Unit],
    private[TraceJob] val _onSuccess: Json => F[Unit],
    private[TraceJob] val _onFailure: Json => F[Unit],
    private[TraceJob] val _onCancel: Json => F[Unit],
    private[TraceJob] val _onError: (Json, Throwable) => F[Unit]
  ) {
    private def copy(
      _onKickoff: Json => F[Unit] = this._onKickoff,
      _onSuccess: Json => F[Unit] = this._onSuccess,
      _onFailure: Json => F[Unit] = this._onFailure
    ): Builder[F] =
      new Builder[F](agent)(
        _onKickoff = _onKickoff,
        _onSuccess = _onSuccess,
        _onFailure = _onFailure,
        _onCancel = this._onCancel,
        _onError = this._onError)

    object anchor {
      object herald {
        val warn: Json => F[Unit] = agent.herald.warn(_)
        val done: Json => F[Unit] = agent.herald.done(_)
        val info: Json => F[Unit] = agent.herald.info(_)
      }
      object console {
        val warn: Json => F[Unit] = agent.console.warn(_)
        val done: Json => F[Unit] = agent.console.done(_)
        val info: Json => F[Unit] = agent.console.info(_)
      }
      val error: Json => F[Unit] = agent.herald.error(_)
      val debug: Json => F[Unit] = agent.console.debug(_)
      val void: Json => F[Unit]  = agent.console.void(_)
    }

    def sendKickoffTo(f: anchor.type => Json => F[Unit]): Builder[F] =
      copy(_onKickoff = f(anchor))

    def sendSuccessTo(f: anchor.type => Json => F[Unit]): Builder[F] =
      copy(_onSuccess = f(anchor))

    def sendFailureTo(f: anchor.type => Json => F[Unit]): Builder[F] =
      copy(_onFailure = f(anchor))

    private[TraceJob] def buildWith[A](translate: (JobOutcome, A) => Json): JsonTracer[F, A] =
      new JsonTracer[F, A](
        _translate = translate,
        _onKickoff = this._onKickoff,
        _onFailure = this._onFailure,
        _onSuccess = this._onSuccess,
        _onCancel = this._onCancel,
        _onError = this._onError,
        _predicate = _ => true
      )
  }

  private object Builder {
    def apply[F[_]](agent: Agent[F]) =
      new Builder[F](agent)(
        _onKickoff = agent.console.debug(_),
        _onSuccess = agent.console.done(_),
        _onFailure = agent.herald.warn(_),
        _onCancel = agent.console.warn(_),
        _onError = (job, ex) => agent.herald.error(ex)(job)
      )
  }

  def json[F[_], A: Encoder](agent: Agent[F], f: Endo[Builder[F]] = identity[Builder[F]]): JsonTracer[F, A] =
    f(Builder(agent)).buildWith[A]((_, a) => Encoder[A].apply(a))

  def dataRate[F[_]](
    agent: Agent[F],
    f: Endo[Builder[F]] = identity[Builder[F]]): JsonTracer[F, Information] = {
    def translate(job: JobOutcome, number: Information): Json =
      jsonDataRate(job.took, number)

    f(Builder(agent)).buildWith[Information](translate)
  }

  def scalarRate[F[_]](
    agent: Agent[F],
    f: Endo[Builder[F]] = identity[Builder[F]]): JsonTracer[F, Dimensionless] = {
    def translate(job: JobOutcome, number: Dimensionless): Json =
      jsonScalarRate(job.took, number)

    f(Builder(agent)).buildWith[Dimensionless](translate)
  }
}
