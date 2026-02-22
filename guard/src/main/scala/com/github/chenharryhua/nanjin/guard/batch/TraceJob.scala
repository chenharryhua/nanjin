package com.github.chenharryhua.nanjin.guard.batch

import cats.effect.kernel.{MonadCancel, Resource}
import cats.syntax.flatMap.catsSyntaxFlatMapOps
import cats.{Applicative, Monoid}
import com.github.chenharryhua.nanjin.guard.logging.Log
import io.circe.syntax.EncoderOps
import io.circe.{Encoder, Json}

sealed trait TraceJob[F[_], A] {
  private[batch] def kickoff(bj: BatchJob): F[Unit]
  private[batch] def canceled(bj: BatchJob): F[Unit]
  private[batch] def completed(jrv: JobResultValue[A]): F[Unit]
  private[batch] def errored(jre: JobResultError): F[Unit]
}

object TraceJob {
  sealed protected trait EventHandle[F[_], A] {
    def onComplete(f: JobResultValue[A] => F[Unit]): EventHandle[F, A]
    def onError(f: JobResultError => F[Unit]): EventHandle[F, A]
    def onCancel(f: BatchJob => F[Unit]): EventHandle[F, A]
    def onKickoff(f: BatchJob => F[Unit]): EventHandle[F, A]
  }

  final class JobTracer[F[_], A] private[TraceJob] (
    _completed: JobResultValue[A] => F[Unit],
    _errored: JobResultError => F[Unit],
    _canceled: BatchJob => F[Unit],
    _kickoff: BatchJob => F[Unit]
  ) extends TraceJob[F, A] with EventHandle[F, A] {
    override private[batch] def kickoff(bj: BatchJob): F[Unit] = _kickoff(bj)
    override private[batch] def canceled(bj: BatchJob): F[Unit] = _canceled(bj)
    override private[batch] def completed(jrv: JobResultValue[A]): F[Unit] = _completed(jrv)
    override private[batch] def errored(jre: JobResultError): F[Unit] = _errored(jre)

    private def copy(
      _completed: JobResultValue[A] => F[Unit] = this._completed,
      _errored: JobResultError => F[Unit] = this._errored,
      _canceled: BatchJob => F[Unit] = this._canceled,
      _kickoff: BatchJob => F[Unit] = this._kickoff): JobTracer[F, A] =
      new JobTracer[F, A](_completed, _errored, _canceled, _kickoff)

    override def onComplete(f: JobResultValue[A] => F[Unit]): JobTracer[F, A] = copy(_completed = f)
    override def onError(f: JobResultError => F[Unit]): JobTracer[F, A] = copy(_errored = f)
    override def onCancel(f: BatchJob => F[Unit]): JobTracer[F, A] = copy(_canceled = f)
    override def onKickoff(f: BatchJob => F[Unit]): JobTracer[F, A] = copy(_kickoff = f)

    def contramap[B](f: B => A): JobTracer[F, B] =
      new JobTracer[F, B](
        _completed = (jrv: JobResultValue[B]) => _completed(jrv.map(f)),
        _errored = this._errored,
        _canceled = this._canceled,
        _kickoff = this._kickoff
      )
  }

  private def _empty[F[_], A](implicit F: Applicative[F]) =
    new JobTracer[F, A](
      _completed = _ => F.unit,
      _errored = _ => F.unit,
      _canceled = _ => F.unit,
      _kickoff = _ => F.unit
    )

  def noop[F[_]: Applicative, A]: Resource[F, JobTracer[F, A]] =
    Resource.pure[F, JobTracer[F, A]](_empty)

  final class ByLogger[F[_]] private[TraceJob] (logger: Resource[F, Log[F]]) {

    def universal[A](f: (A, JobResultState) => Json): Resource[F, JobTracer[F, A]] =
      logger.map { log =>
        new JobTracer[F, A](
          _completed = { (jrv: JobResultValue[A]) =>
            val json: Json =
              Json.obj("outcome" -> f(jrv.value, jrv.resultState)).deepMerge(jrv.resultState.asJson)
            if (jrv.resultState.done) log.done(Json.obj("done" -> json))
            else log.warn(Json.obj("fail" -> json))
          },
          _errored = (jre: JobResultError) => log.error(jre.error)(jre.resultState),
          _canceled = (bj: BatchJob) => log.warn(Json.obj("canceled" -> bj.asJson)),
          _kickoff = (bj: BatchJob) => log.info(Json.obj("kickoff" -> bj.asJson))
        )
      }

    def standard[A: Encoder]: Resource[F, JobTracer[F, A]] =
      universal[A]((a, _) => a.asJson)

    def json: Resource[F, JobTracer[F, Json]] = standard[Json]
  }

  def apply[F[_]](logger: Resource[F, Log[F]]): ByLogger[F] =
    new ByLogger[F](logger)

  implicit def monoidTraceJob[F[_], A](implicit F: MonadCancel[F, Throwable]): Monoid[TraceJob[F, A]] =
    new Monoid[TraceJob[F, A]] {

      override val empty: TraceJob[F, A] = _empty[F, A]

      override def combine(x: TraceJob[F, A], y: TraceJob[F, A]): TraceJob[F, A] =
        new TraceJob[F, A] {
          override private[batch] def kickoff(bj: BatchJob): F[Unit] =
            F.uncancelable(_ => x.kickoff(bj) >> y.kickoff(bj))

          override private[batch] def canceled(bj: BatchJob): F[Unit] =
            F.uncancelable(_ => x.canceled(bj) >> y.canceled(bj))

          override private[batch] def completed(jrv: JobResultValue[A]): F[Unit] =
            F.uncancelable(_ => x.completed(jrv) >> y.completed(jrv))

          override private[batch] def errored(jre: JobResultError): F[Unit] =
            F.uncancelable(_ => x.errored(jre) >> y.errored(jre))
        }
    }
}
