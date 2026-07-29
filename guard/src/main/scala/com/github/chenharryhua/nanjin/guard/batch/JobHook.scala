package com.github.chenharryhua.nanjin.guard.batch

import cats.effect.kernel.MonadCancel
import cats.syntax.flatMap.given
import cats.{Applicative, Contravariant, Monoid}
import com.github.chenharryhua.nanjin.guard.service.logging.Log
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
  private[batch] def kickoff: Job => F[Unit]
  private[batch] def canceled: Job => F[Unit]
  private[batch] def completed: JobState[A] => F[Unit]

object JobHook {

  /** `Subscriber` represents a set of callbacks that can be registered to observe lifecycle events of a
    * `JobHook`. It allows clients to react when a job:
    *
    *   - is kicked off (`onKickoff`)
    *   - completes (`onComplete`)
    *   - is canceled (`onCancel`)
    *
    * This trait is protected and intended for internal use within the JobHook system, providing a functional,
    * composable way to handle job events.
    */
  sealed protected trait Subscriber[F[_], A]:
    def onComplete(f: JobState[A] => F[Unit]): Bridge[F, A]
    def onCancel(f: Job => F[Unit]): Bridge[F, A]
    def onKickoff(f: Job => F[Unit]): Bridge[F, A]

  /** Concrete implementation of JobHook that bridges internal events to subscriber callbacks. Supports
    * contramap for type transformations.
    */
  final class Bridge[F[_], A] private[JobHook] (
    private[batch] val completed: JobState[A] => F[Unit],
    private[batch] val canceled: Job => F[Unit],
    private[batch] val kickoff: Job => F[Unit]
  ) extends JobHook[F, A] with Subscriber[F, A] {

    private def copy(
      completed: JobState[A] => F[Unit] = this.completed,
      canceled: Job => F[Unit] = this.canceled,
      kickoff: Job => F[Unit] = this.kickoff): Bridge[F, A] =
      new Bridge[F, A](completed, canceled, kickoff)

    override def onComplete(f: JobState[A] => F[Unit]): Bridge[F, A] = copy(completed = f)
    override def onCancel(f: Job => F[Unit]): Bridge[F, A] = copy(canceled = f)
    override def onKickoff(f: Job => F[Unit]): Bridge[F, A] = copy(kickoff = f)

    def contramap[B](f: B => A): Bridge[F, B] =
      new Bridge[F, B](
        completed = (jrv: JobState[B]) => this.completed(jrv.map(f)),
        canceled = this.canceled,
        kickoff = this.kickoff
      )
  }

  /*
   * Concrete, configured Bridges
   */

  def noop[F[_], A](using F: Applicative[F]): Bridge[F, A] =
    new Bridge[F, A](
      completed = _ => F.unit,
      canceled = _ => F.unit,
      kickoff = _ => F.unit
    )

  def apply[F[_]](logger: Log[F]): ByLogger[F] = ByLogger[F](logger)

  /*
   * Bridge by logger
   */

  final class ByLogger[F[_]](log: Log[F]) {

    def universal[A](f: JobState[A] => Json): Bridge[F, A] =
      new Bridge[F, A](
        completed = { (js: JobState[A]) =>
          js.result match {
            case Left(ex) => log.warn(Json.obj("fail" -> f(js)), ex)
            case Right(_) => log.good(Json.obj("done" -> f(js)))
          }
        },
        canceled = (bj: Job) => log.warn(Json.obj("canceled" -> bj.asJson)),
        kickoff = (bj: Job) => log.info(Json.obj("kickoff" -> bj.asJson))
      )

    def standard[A: Encoder]: Bridge[F, A] = universal[A](_.asJson)

    def json: Bridge[F, Json] = standard[Json]
  }

  given monoidBridge[F[_], A](using F: MonadCancel[F, Throwable]): Monoid[Bridge[F, A]] =
    new Monoid[Bridge[F, A]] {

      override val empty: Bridge[F, A] = noop[F, A]

      override def combine(x: Bridge[F, A], y: Bridge[F, A]): Bridge[F, A] =
        new Bridge[F, A](
          completed = jrv => F.uncancelable(_ => x.completed(jrv) >> y.completed(jrv)),
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
