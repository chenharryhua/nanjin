package com.github.chenharryhua.nanjin.guard.action

import cats.implicits.catsSyntaxFlatMapOps
import cats.{Applicative, Monad, Monoid}
import com.github.chenharryhua.nanjin.guard.service.Agent
import io.circe.syntax.EncoderOps
import io.circe.{Encoder, Json}
import squants.Dimensionless
import squants.information.Information

sealed trait TraceJob[F[_], A] {
  private[action] def kickoff(bj: BatchJob): F[Unit]
  private[action] def canceled(bj: BatchJob): F[Unit]
  private[action] def completed(a: A, jrs: JobResultState): F[Unit]
  private[action] def errored(ex: Throwable, jrs: JobResultState): F[Unit]
}

object TraceJob {

  def noop[F[_], A](implicit F: Applicative[F]): TraceJob[F, A] =
    new TraceJob[F, A] {
      override private[action] def errored(ex: Throwable, jrs: JobResultState): F[Unit] = F.unit
      override private[action] def completed(a: A, jrs: JobResultState): F[Unit]        = F.unit
      override private[action] def canceled(bj: BatchJob): F[Unit]                      = F.unit
      override private[action] def kickoff(bj: BatchJob): F[Unit]                       = F.unit
    }

  final class GenericTracer[F[_], A] private[TraceJob] (
    private val _completed: (A, JobResultState) => F[Unit],
    private val _errored: (Throwable, JobResultState) => F[Unit],
    private val _canceled: BatchJob => F[Unit],
    private val _kickoff: BatchJob => F[Unit]
  ) extends TraceJob[F, A] {
    override private[action] def kickoff(bj: BatchJob): F[Unit]                       = _kickoff(bj)
    override private[action] def canceled(bj: BatchJob): F[Unit]                      = _canceled(bj)
    override private[action] def completed(a: A, jrs: JobResultState): F[Unit]        = _completed(a, jrs)
    override private[action] def errored(ex: Throwable, jrs: JobResultState): F[Unit] = _errored(ex, jrs)

    private def copy(
      _completed: (A, JobResultState) => F[Unit] = this._completed,
      _errored: (Throwable, JobResultState) => F[Unit] = this._errored,
      _canceled: BatchJob => F[Unit] = this._canceled,
      _kickoff: BatchJob => F[Unit] = this._kickoff): GenericTracer[F, A] =
      new GenericTracer[F, A](_completed, _errored, _canceled, _kickoff)

    def contramap[B](f: B => A): GenericTracer[F, B] =
      new GenericTracer[F, B](
        _completed = (b, job) => _completed(f(b), job),
        _errored = this._errored,
        _canceled = this._canceled,
        _kickoff = this._kickoff
      )

    def onComplete(f: (A, JobResultState) => F[Unit]): GenericTracer[F, A]      = copy(_completed = f)
    def onError(f: (Throwable, JobResultState) => F[Unit]): GenericTracer[F, A] = copy(_errored = f)
    def onCancel(f: BatchJob => F[Unit]): GenericTracer[F, A]                   = copy(_canceled = f)
    def onKickoff(f: BatchJob => F[Unit]): GenericTracer[F, A]                  = copy(_kickoff = f)
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
    _translate: (A, JobResultState) => Json,
    _kickoff: Json => F[Unit],
    _failure: Json => F[Unit],
    _success: Json => F[Unit],
    _canceled: Json => F[Unit],
    _errored: (Throwable, Json) => F[Unit]
  ) extends TraceJob[F, A] {
    override private[action] def kickoff(bj: BatchJob): F[Unit] =
      _kickoff(Json.obj("kickoff" -> bj.asJson))

    override private[action] def canceled(bj: BatchJob): F[Unit] =
      _canceled(Json.obj("canceled" -> bj.asJson))

    override private[action] def completed(a: A, jrs: JobResultState): F[Unit] = {
      val json = _translate(a, jrs)
      if (jrs.done) _success(json) else _failure(json)
    }

    override private[action] def errored(ex: Throwable, jrs: JobResultState): F[Unit] =
      _errored(ex, jrs.asJson)

    def contramap[B](f: B => A): JsonTracer[F, B] =
      new JsonTracer[F, B](
        _translate = (b, jo) => this._translate(f(b), jo),
        _kickoff = this._kickoff,
        _failure = this._failure,
        _success = this._success,
        _canceled = this._canceled,
        _errored = this._errored
      )
  }

  private object JsonTracer {
    def apply[F[_], A](translate: (A, JobResultState) => Json)(redirect: Redirect[F]): JsonTracer[F, A] =
      new JsonTracer[F, A](
        _translate = translate,
        _kickoff = redirect._kickoff,
        _failure = redirect._failure,
        _success = redirect._success,
        _canceled = redirect._canceled,
        _errored = redirect._errored
      )
  }

  final class Redirect[F[_]] private[TraceJob] (
    private[TraceJob] val _agent: Agent[F],
    private[TraceJob] val _kickoff: Json => F[Unit],
    private[TraceJob] val _failure: Json => F[Unit],
    private[TraceJob] val _success: Json => F[Unit],
    private[TraceJob] val _canceled: Json => F[Unit],
    private[TraceJob] val _errored: (Throwable, Json) => F[Unit]) {
    private def copy(
      _kickoff: Json => F[Unit] = this._kickoff,
      _failure: Json => F[Unit] = this._failure,
      _success: Json => F[Unit] = this._success
    ): Redirect[F] = new Redirect[F](
      _agent = this._agent,
      _kickoff = _kickoff,
      _failure = _failure,
      _success = _success,
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

    def sendKickoffTo(f: anchor.type => Json => F[Unit]): Redirect[F] =
      copy(_kickoff = f(anchor))

    def sendSuccessTo(f: anchor.type => Json => F[Unit]): Redirect[F] =
      copy(_success = f(anchor))

    def sendFailureTo(f: anchor.type => Json => F[Unit]): Redirect[F] =
      copy(_failure = f(anchor))

    def universal[A](f: (A, JobResultState) => Json): JsonTracer[F, A] =
      JsonTracer[F, A](f)(this)

    def standard[A: Encoder]: JsonTracer[F, A] =
      universal[A]((a, jrs) => Json.obj("outcome" -> a.asJson).deepMerge(jrs.asJson))

    def json: JsonTracer[F, Json] = standard[Json]

    def dataRate: JsonTracer[F, Information] = {
      def translate(number: Information, jrs: JobResultState): Json =
        Json.obj("outcome" -> jsonDataRate(jrs.took, number)).deepMerge(jrs.asJson)

      universal[Information](translate)
    }

    def scalarRate: JsonTracer[F, Dimensionless] = {
      def translate(number: Dimensionless, jrs: JobResultState): Json =
        Json.obj("outcome" -> jsonScalarRate(jrs.took, number)).deepMerge(jrs.asJson)

      universal[Dimensionless](translate)
    }
  }

  def apply[F[_]](agent: Agent[F]): Redirect[F] =
    new Redirect[F](
      _agent = agent,
      _kickoff = agent.console.info(_),
      _failure = agent.herald.warn(_),
      _success = agent.console.done(_),
      _canceled = agent.console.warn(_),
      _errored = (ex, js) => agent.herald.error(ex)(js)
    )

  implicit def monoidTraceJob[F[_]: Monad, A]: Monoid[TraceJob[F, A]] =
    new Monoid[TraceJob[F, A]] {

      override val empty: TraceJob[F, A] = noop[F, A]

      override def combine(x: TraceJob[F, A], y: TraceJob[F, A]): TraceJob[F, A] =
        new TraceJob[F, A] {
          override private[action] def kickoff(bj: BatchJob): F[Unit] =
            x.kickoff(bj) >> y.kickoff(bj)

          override private[action] def canceled(bj: BatchJob): F[Unit] =
            x.canceled(bj) >> y.canceled(bj)

          override private[action] def completed(a: A, jrs: JobResultState): F[Unit] =
            x.completed(a, jrs) >> y.completed(a, jrs)

          override private[action] def errored(ex: Throwable, jrs: JobResultState): F[Unit] =
            x.errored(ex, jrs) >> y.errored(ex, jrs)
        }
    }
}
