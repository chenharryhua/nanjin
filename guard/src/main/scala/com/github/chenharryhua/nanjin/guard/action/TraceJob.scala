package com.github.chenharryhua.nanjin.guard.action

import cats.implicits.catsSyntaxFlatMapOps
import cats.{Applicative, Monad, Monoid}
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
}

object TraceJob {

  def noop[F[_], A](implicit F: Applicative[F]): TraceJob[F, A] =
    new TraceJob[F, A] {
      override private[action] def errored(jo: JobOutcome, ex: Throwable): F[Unit] = F.unit
      override private[action] def completed(jo: JobOutcome, a: A): F[Unit]        = F.unit
      override private[action] def canceled(bj: BatchJobID): F[Unit]               = F.unit
      override private[action] def kickoff(bj: BatchJobID): F[Unit]                = F.unit
    }

  final class GenericTracer[F[_], A] private[TraceJob] (
    private val _completed: (JobOutcome, A) => F[Unit],
    private val _errored: (JobOutcome, Throwable) => F[Unit],
    private val _canceled: BatchJobID => F[Unit],
    private val _kickoff: BatchJobID => F[Unit]
  ) extends TraceJob[F, A] {
    override private[action] def kickoff(bj: BatchJobID): F[Unit]                = _kickoff(bj)
    override private[action] def canceled(bj: BatchJobID): F[Unit]               = _canceled(bj)
    override private[action] def completed(jo: JobOutcome, a: A): F[Unit]        = _completed(jo, a)
    override private[action] def errored(jo: JobOutcome, ex: Throwable): F[Unit] = _errored(jo, ex)

    private def copy(
      _completed: (JobOutcome, A) => F[Unit] = this._completed,
      _errored: (JobOutcome, Throwable) => F[Unit] = this._errored,
      _canceled: BatchJobID => F[Unit] = this._canceled,
      _kickoff: BatchJobID => F[Unit] = this._kickoff): GenericTracer[F, A] =
      new GenericTracer[F, A](_completed, _errored, _canceled, _kickoff)

    def contramap[B](f: B => A): GenericTracer[F, B] =
      new GenericTracer[F, B](
        _completed = (job, b) => _completed(job, f(b)),
        _errored,
        _canceled,
        _kickoff
      )

    def onComplete(f: (JobOutcome, A) => F[Unit]): GenericTracer[F, A]      = copy(_completed = f)
    def onError(f: (JobOutcome, Throwable) => F[Unit]): GenericTracer[F, A] = copy(_errored = f)
    def onCancel(f: BatchJobID => F[Unit]): GenericTracer[F, A]             = copy(_canceled = f)
    def onKickoff(f: BatchJobID => F[Unit]): GenericTracer[F, A]            = copy(_kickoff = f)
  }

  def generic[F[_], A](implicit F: Applicative[F]): GenericTracer[F, A] =
    new GenericTracer[F, A](
      _completed = (_, _) => F.unit,
      _errored = (_, _) => F.unit,
      _canceled = _ => F.unit,
      _kickoff = _ => F.unit
    )

  /*
   * Trace job using json format
   */

  final class JsonTracer[F[_], A] private (
    private val _agent: Agent[F],
    private val _translate: (JobOutcome, A) => Json,
    private val _kickoff: Json => F[Unit],
    private val _failure: Json => F[Unit],
    private val _success: Json => F[Unit],
    private val _canceled: Json => F[Unit],
    private val _errored: (Json, Throwable) => F[Unit]
  ) extends TraceJob[F, A] {
    override private[action] def kickoff(bj: BatchJobID): F[Unit] =
      _kickoff(Json.obj("kickoff" -> bj.asJson))

    override private[action] def canceled(bj: BatchJobID): F[Unit] =
      _canceled(Json.obj("canceled" -> bj.asJson))

    override private[action] def completed(jo: JobOutcome, a: A): F[Unit] = {
      val json = Json.obj("outcome" -> _translate(jo, a)).deepMerge(jo.asJson)
      if (jo.done) _success(json) else _failure(json)
    }

    override private[action] def errored(jo: JobOutcome, ex: Throwable): F[Unit] =
      _errored(jo.asJson, ex)

    def contramap[B](f: B => A): JsonTracer[F, B] =
      new JsonTracer[F, B](
        _agent = this._agent,
        _translate = (jo, b) => this._translate(jo, f(b)),
        _kickoff = this._kickoff,
        _failure = this._failure,
        _success = this._success,
        _canceled = this._canceled,
        _errored = this._errored
      )

    object anchor {
      object herald {
        val warn: Json => F[Unit] = _agent.herald.warn(_)
        val done: Json => F[Unit] = _agent.herald.done(_)
        val info: Json => F[Unit] = _agent.herald.info(_)
      }
      object console {
        val warn: Json => F[Unit] = _agent.console.warn(_)
        val done: Json => F[Unit] = _agent.console.done(_)
        val info: Json => F[Unit] = _agent.console.info(_)
      }
      val error: Json => F[Unit] = _agent.herald.error(_)
      val debug: Json => F[Unit] = _agent.console.debug(_)
      val void: Json => F[Unit]  = _agent.console.void(_)
    }

    private def copy(
      _kickoff: Json => F[Unit] = this._kickoff,
      _failure: Json => F[Unit] = this._failure,
      _success: Json => F[Unit] = this._success
    ): JsonTracer[F, A] = new JsonTracer[F, A](
      _agent = this._agent,
      _translate = this._translate,
      _kickoff = _kickoff,
      _failure = _failure,
      _success = _success,
      _canceled = this._canceled,
      _errored = this._errored
    )

    def sendKickoffTo(f: anchor.type => Json => F[Unit]): JsonTracer[F, A] =
      copy(_kickoff = f(anchor))

    def sendSuccessTo(f: anchor.type => Json => F[Unit]): JsonTracer[F, A] =
      copy(_success = f(anchor))

    def sendFailureTo(f: anchor.type => Json => F[Unit]): JsonTracer[F, A] =
      copy(_failure = f(anchor))
  }

  private object JsonTracer {
    def apply[F[_], A](agent: Agent[F], translate: (JobOutcome, A) => Json): JsonTracer[F, A] =
      new JsonTracer[F, A](
        _agent = agent,
        _translate = translate,
        _kickoff = agent.console.info(_),
        _failure = agent.herald.warn(_),
        _success = agent.console.done(_),
        _canceled = agent.console.warn(_),
        _errored = (js, ex) => agent.herald.error(ex)(js)
      )
  }

  def standard[F[_], A: Encoder](agent: Agent[F]): JsonTracer[F, A] =
    JsonTracer[F, A](agent, (_, a) => Encoder[A].apply(a))

  def json[F[_]](agent: Agent[F]): JsonTracer[F, Json] =
    JsonTracer[F, Json](agent, (_, js) => js)

  def dataRate[F[_]](agent: Agent[F]): JsonTracer[F, Information] = {
    def translate(job: JobOutcome, number: Information): Json =
      jsonDataRate(job.took, number)

    JsonTracer[F, Information](agent, translate)
  }

  def scalarRate[F[_]](agent: Agent[F]): JsonTracer[F, Dimensionless] = {
    def translate(job: JobOutcome, number: Dimensionless): Json =
      jsonScalarRate(job.took, number)

    JsonTracer[F, Dimensionless](agent, translate)
  }

  implicit def monoidTraceJob[F[_]: Monad, A]: Monoid[TraceJob[F, A]] = new Monoid[TraceJob[F, A]] {

    override val empty: TraceJob[F, A] = noop[F, A]

    override def combine(x: TraceJob[F, A], y: TraceJob[F, A]): TraceJob[F, A] = new TraceJob[F, A] {
      override private[action] def kickoff(bj: BatchJobID): F[Unit] =
        x.kickoff(bj) >> y.kickoff(bj)

      override private[action] def canceled(bj: BatchJobID): F[Unit] =
        x.canceled(bj) >> y.canceled(bj)

      override private[action] def completed(jo: JobOutcome, a: A): F[Unit] =
        x.completed(jo, a) >> y.completed(jo, a)

      override private[action] def errored(jo: JobOutcome, ex: Throwable): F[Unit] =
        x.errored(jo, ex) >> y.errored(jo, ex)
    }
  }
}
