package com.github.chenharryhua.nanjin.guard.batch

import cats.effect.kernel.{MonadCancel, Resource}
import cats.syntax.flatMap.catsSyntaxFlatMapOps
import cats.{Applicative, Contravariant, Monoid}
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
sealed trait JobHook[F[_], A]:
  private[batch] def kickoff: BatchJob => F[Unit]
  private[batch] def canceled: BatchJob => F[Unit]
  private[batch] def completed: JobResultValue[A] => F[Unit]
  private[batch] def errored: JobResultError => F[Unit]

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
  sealed protected trait Subscriber[F[_], A]:
    def onComplete(f: JobResultValue[A] => F[Unit]): Subscriber[F, A]
    def onError(f: JobResultError => F[Unit]): Subscriber[F, A]
    def onCancel(f: BatchJob => F[Unit]): Subscriber[F, A]
    def onKickoff(f: BatchJob => F[Unit]): Subscriber[F, A]

  /** Concrete implementation of JobHook that bridges internal events to subscriber callbacks. Supports
    * contramap for type transformations.
    */
  final class Bridge[F[_], A] private[JobHook] (
    private[batch] val completed: JobResultValue[A] => F[Unit],
    private[batch] val errored: JobResultError => F[Unit],
    private[batch] val canceled: BatchJob => F[Unit],
    private[batch] val kickoff: BatchJob => F[Unit]
  ) extends JobHook[F, A] with Subscriber[F, A] {

    private def copy(
      completed: JobResultValue[A] => F[Unit] = this.completed,
      errored: JobResultError => F[Unit] = this.errored,
      canceled: BatchJob => F[Unit] = this.canceled,
      kickoff: BatchJob => F[Unit] = this.kickoff): Bridge[F, A] =
      new Bridge[F, A](completed, errored, canceled, kickoff)

    override def onComplete(f: JobResultValue[A] => F[Unit]): Bridge[F, A] = copy(completed = f)
    override def onError(f: JobResultError => F[Unit]): Bridge[F, A] = copy(errored = f)
    override def onCancel(f: BatchJob => F[Unit]): Bridge[F, A] = copy(canceled = f)
    override def onKickoff(f: BatchJob => F[Unit]): Bridge[F, A] = copy(kickoff = f)

    def contramap[B](f: B => A): Bridge[F, B] =
      new Bridge[F, B](
        completed = (jrv: JobResultValue[B]) => this.completed(jrv.map(f)),
        errored = this.errored,
        canceled = this.canceled,
        kickoff = this.kickoff
      )
  }

  /*
   * Concrete, configured Bridges
   */

  private def _emptyBridge[F[_], A](using F: Applicative[F]) =
    new Bridge[F, A](
      completed = _ => F.unit,
      errored = _ => F.unit,
      canceled = _ => F.unit,
      kickoff = _ => F.unit
    )

  def noop[F[_]: Applicative, A]: Resource[F, Bridge[F, A]] =
    Resource.pure[F, Bridge[F, A]](_emptyBridge)

  final class ByLogger[F[_]] private[JobHook] (logger: Resource[F, Log[F]]) {

    def universal[A](f: (A, JobResultState) => Json): Resource[F, Bridge[F, A]] =
      logger.map { log =>
        new Bridge[F, A](
          completed = { (jrv: JobResultValue[A]) =>
            val json: Json =
              Json.obj("outcome" -> f(jrv.value, jrv.resultState)).deepMerge(jrv.resultState.asJson)
            if (jrv.resultState.done) log.good(Json.obj("done" -> json))
            else log.warn(Json.obj("fail" -> json))
          },
          errored = (jre: JobResultError) => log.error(jre.resultState, jre.error),
          canceled = (bj: BatchJob) => log.warn(Json.obj("canceled" -> bj.asJson)),
          kickoff = (bj: BatchJob) => log.info(Json.obj("kickoff" -> bj.asJson))
        )
      }

    def standard[A: Encoder]: Resource[F, Bridge[F, A]] =
      universal[A]((a, _) => a.asJson)

    def json: Resource[F, Bridge[F, Json]] = standard[Json]
  }

  def apply[F[_]](logger: Resource[F, Log[F]]): ByLogger[F] =
    new ByLogger[F](logger)

  given monoidBridge[F[_], A](using F: MonadCancel[F, Throwable]): Monoid[Bridge[F, A]] =
    new Monoid[Bridge[F, A]] {

      override val empty: Bridge[F, A] = _emptyBridge[F, A]

      override def combine(x: Bridge[F, A], y: Bridge[F, A]): Bridge[F, A] =
        new Bridge[F, A](
          completed = jrv => F.uncancelable(_ => x.completed(jrv) >> y.completed(jrv)),
          errored = jre => F.uncancelable(_ => x.errored(jre) >> y.errored(jre)),
          canceled = bj => F.uncancelable(_ => x.canceled(bj) >> y.canceled(bj)),
          kickoff = bj => F.uncancelable(_ => x.kickoff(bj) >> y.kickoff(bj))
        )
    }

  given [F[_]]: Contravariant[Bridge[F, *]] =
    new Contravariant[Bridge[F, *]] {
      override def contramap[A, B](fa: Bridge[F, A])(f: B => A): Bridge[F, B] =
        fa.contramap(f)
    }
}
