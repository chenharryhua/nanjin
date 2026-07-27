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
  private[batch] def kickoff: BatchJob => F[Unit]
  private[batch] def canceled: BatchJob => F[Unit]
  private[batch] def completed: JobValue[A] => F[Unit]
  private[batch] def errored: JobError => F[Unit]

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
    def onComplete(f: JobValue[A] => F[Unit]): Subscriber[F, A]
    def onError(f: JobError => F[Unit]): Subscriber[F, A]
    def onCancel(f: BatchJob => F[Unit]): Subscriber[F, A]
    def onKickoff(f: BatchJob => F[Unit]): Subscriber[F, A]

  /** Concrete implementation of JobHook that bridges internal events to subscriber callbacks. Supports
    * contramap for type transformations.
    */
  final class Bridge[F[_], A] private[JobHook] (
    private[batch] val completed: JobValue[A] => F[Unit],
    private[batch] val errored: JobError => F[Unit],
    private[batch] val canceled: BatchJob => F[Unit],
    private[batch] val kickoff: BatchJob => F[Unit]
  ) extends JobHook[F, A] with Subscriber[F, A] {

    private def copy(
      completed: JobValue[A] => F[Unit] = this.completed,
      errored: JobError => F[Unit] = this.errored,
      canceled: BatchJob => F[Unit] = this.canceled,
      kickoff: BatchJob => F[Unit] = this.kickoff): Bridge[F, A] =
      new Bridge[F, A](completed, errored, canceled, kickoff)

    override def onComplete(f: JobValue[A] => F[Unit]): Bridge[F, A] = copy(completed = f)
    override def onError(f: JobError => F[Unit]): Bridge[F, A] = copy(errored = f)
    override def onCancel(f: BatchJob => F[Unit]): Bridge[F, A] = copy(canceled = f)
    override def onKickoff(f: BatchJob => F[Unit]): Bridge[F, A] = copy(kickoff = f)

    def contramap[B](f: B => A): Bridge[F, B] =
      new Bridge[F, B](
        completed = (jrv: JobValue[B]) => this.completed(jrv.map(f)),
        errored = this.errored,
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
      errored = _ => F.unit,
      canceled = _ => F.unit,
      kickoff = _ => F.unit
    )

  def apply[F[_]](logger: Log[F]): ByLogger[F] = ByLogger[F](logger)

  /*
   * Bridge by logger
   */

  final class ByLogger[F[_]](log: Log[F]) {

    def universal[A](f: (A, JobState) => Json): Bridge[F, A] =
      new Bridge[F, A](
        completed = { (jrv: JobValue[A]) =>
          val json: Json =
            Json.obj("outcome" -> f(jrv.value, jrv.state)).deepMerge(jrv.state.asJson)
          if (jrv.state.done) log.good(Json.obj("done" -> json))
          else log.warn(Json.obj("fail" -> json))
        },
        errored = (jre: JobError) => log.error(jre.state, jre.cause),
        canceled = (bj: BatchJob) => log.warn(Json.obj("canceled" -> bj.asJson)),
        kickoff = (bj: BatchJob) => log.info(Json.obj("kickoff" -> bj.asJson))
      )

    def standard[A: Encoder]: Bridge[F, A] =
      universal[A]((a, _) => a.asJson)

    def json: Bridge[F, Json] = standard[Json]
  }

  given monoidBridge[F[_], A](using F: MonadCancel[F, Throwable]): Monoid[Bridge[F, A]] =
    new Monoid[Bridge[F, A]] {

      override val empty: Bridge[F, A] = noop[F, A]

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
