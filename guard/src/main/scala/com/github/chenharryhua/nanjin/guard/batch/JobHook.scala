package com.github.chenharryhua.nanjin.guard.batch

import cats.effect.kernel.{MonadCancel, Resource}
import cats.syntax.apply.catsSyntaxApplyOps
import cats.{Applicative, Monoid}
import com.github.chenharryhua.nanjin.guard.logging.Log
import io.circe.syntax.EncoderOps
import io.circe.{Encoder, Json}

/** `JobHook` is the internal SPI for observing job lifecycle events. Implementations receive notifications
  * when a job:
  *
  *   - starts execution (`kickoff`)
  *   - completes (`completed`) not necessarily success
  *   - fails with an exception (`errored`)
  *   - is canceled (`canceled`)
  *
  * `JobHook` is intended to be combined, composed, and traced within the batch system. Concrete bridges (like
  * `Bridge`) or loggers can implement this trait to handle events in a functional and effectful way.
  */
sealed trait JobHook[F[_], A] {
  private[batch] def kickoff(bj: BatchJob): F[Unit]
  private[batch] def canceled(bj: BatchJob): F[Unit]
  private[batch] def completed(jrv: JobResultValue[A]): F[Unit]
  private[batch] def errored(jre: JobResultError): F[Unit]
}

object JobHook {

  /** `Subscriber` represents a set of callbacks that can be registered to observe lifecycle events of a
    * `JobHook`. It allows clients to react when a job:
    *
    *   - is kicked off (`onKickoff`)
    *   - completes (`onComplete`)
    *   - fails (`onError`)
    *   - is canceled (`onCancel`)
    *
    * This trait is protected and intended for internal use within the JobHook system, providing a functional,
    * composable way to handle job events.
    */
  sealed protected trait Subscriber[F[_], A] {
    def onComplete(f: JobResultValue[A] => F[Unit]): Subscriber[F, A]
    def onError(f: JobResultError => F[Unit]): Subscriber[F, A]
    def onCancel(f: BatchJob => F[Unit]): Subscriber[F, A]
    def onKickoff(f: BatchJob => F[Unit]): Subscriber[F, A]
  }

  /** Concrete implementation of JobHook that bridges internal events to subscriber callbacks. Supports
    * contramap for type transformations.
    */
  final class Bridge[F[_], A] private[JobHook] (
    _completed: JobResultValue[A] => F[Unit],
    _errored: JobResultError => F[Unit],
    _canceled: BatchJob => F[Unit],
    _kickoff: BatchJob => F[Unit]
  ) extends JobHook[F, A] with Subscriber[F, A] {
    override private[batch] def kickoff(bj: BatchJob): F[Unit] = _kickoff(bj)
    override private[batch] def canceled(bj: BatchJob): F[Unit] = _canceled(bj)
    override private[batch] def completed(jrv: JobResultValue[A]): F[Unit] = _completed(jrv)
    override private[batch] def errored(jre: JobResultError): F[Unit] = _errored(jre)

    private def copy(
      _completed: JobResultValue[A] => F[Unit] = this._completed,
      _errored: JobResultError => F[Unit] = this._errored,
      _canceled: BatchJob => F[Unit] = this._canceled,
      _kickoff: BatchJob => F[Unit] = this._kickoff): Bridge[F, A] =
      new Bridge[F, A](_completed, _errored, _canceled, _kickoff)

    override def onComplete(f: JobResultValue[A] => F[Unit]): Bridge[F, A] = copy(_completed = f)
    override def onError(f: JobResultError => F[Unit]): Bridge[F, A] = copy(_errored = f)
    override def onCancel(f: BatchJob => F[Unit]): Bridge[F, A] = copy(_canceled = f)
    override def onKickoff(f: BatchJob => F[Unit]): Bridge[F, A] = copy(_kickoff = f)

    def contramap[B](f: B => A): Bridge[F, B] =
      new Bridge[F, B](
        _completed = (jrv: JobResultValue[B]) => _completed(jrv.map(f)),
        _errored = this._errored,
        _canceled = this._canceled,
        _kickoff = this._kickoff
      )
  }

  /*
   * Concrete, configured Bridges
   */

  private def _empty[F[_], A](implicit F: Applicative[F]) =
    new Bridge[F, A](
      _completed = _ => F.unit,
      _errored = _ => F.unit,
      _canceled = _ => F.unit,
      _kickoff = _ => F.unit
    )

  def noop[F[_]: Applicative, A]: Resource[F, Bridge[F, A]] =
    Resource.pure[F, Bridge[F, A]](_empty)

  final class ByLogger[F[_]] private[JobHook] (logger: Resource[F, Log[F]]) {

    def universal[A](f: (A, JobResultState) => Json): Resource[F, Bridge[F, A]] =
      logger.map { log =>
        new Bridge[F, A](
          _completed = { (jrv: JobResultValue[A]) =>
            val json: Json =
              Json.obj("outcome" -> f(jrv.value, jrv.resultState)).deepMerge(jrv.resultState.asJson)
            if (jrv.resultState.done) log.good(Json.obj("done" -> json))
            else log.warn(Json.obj("fail" -> json))
          },
          _errored = (jre: JobResultError) => log.error(jre.resultState, jre.error),
          _canceled = (bj: BatchJob) => log.warn(Json.obj("canceled" -> bj.asJson)),
          _kickoff = (bj: BatchJob) => log.info(Json.obj("kickoff" -> bj.asJson))
        )
      }

    def standard[A: Encoder]: Resource[F, Bridge[F, A]] =
      universal[A]((a, _) => a.asJson)

    def json: Resource[F, Bridge[F, Json]] = standard[Json]
  }

  def apply[F[_]](logger: Resource[F, Log[F]]): ByLogger[F] =
    new ByLogger[F](logger)

  implicit def monoidBridge[F[_], A](implicit F: MonadCancel[F, Throwable]): Monoid[Bridge[F, A]] =
    new Monoid[Bridge[F, A]] {

      override val empty: Bridge[F, A] = _empty[F, A]

      override def combine(x: Bridge[F, A], y: Bridge[F, A]): Bridge[F, A] =
        new Bridge[F, A](
          _completed = jrv => F.uncancelable(_ => x.completed(jrv) *> y.completed(jrv)),
          _errored = jre => F.uncancelable(_ => x.errored(jre) *> y.errored(jre)),
          _canceled = bj => F.uncancelable(_ => x.canceled(bj) *> y.canceled(bj)),
          _kickoff = bj => F.uncancelable(_ => x.kickoff(bj) *> y.kickoff(bj))
        )
    }
}
