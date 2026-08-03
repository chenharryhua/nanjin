package com.github.chenharryhua.nanjin.guard.batch

import cats.effect.kernel.MonadCancel
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import cats.{Applicative, Contravariant, Monoid}
import com.github.chenharryhua.nanjin.common.logging.Log
import io.circe.syntax.EncoderOps
import io.circe.{Encoder, Json}

/** Internal SPI for observing the lifecycle of a batch job. Implementations receive notifications when a job
  * starts, finishes (successfully or with failure), or is canceled, and can turn those events into logs,
  * metrics, tracing, or other side effects.
  */
sealed trait JobHook[F[_], A]:
  private[batch] def kickoff: Job => F[Unit]
  private[batch] def canceled: Job => F[Unit]
  private[batch] def completed: JobState[A] => F[Unit]

object JobHook {

  /** A small callback API for registering interest in specific lifecycle events of a job hook. It is intended
    * for internal composition rather than direct external use.
    */
  sealed protected trait Subscriber[F[_], A]:
    def onComplete(f: JobState[A] => F[Unit]): Bridge[F, A]
    def onCancel(f: Job => F[Unit]): Bridge[F, A]
    def onKickoff(f: Job => F[Unit]): Bridge[F, A]

  /** Concrete implementation of JobHook that forwards lifecycle events to registered subscriber callbacks. It
    * also supports contravariant mapping for adapting the payload type of completed jobs.
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
          val json: Json = f(js)
          js.result match {
            case Left(ex) =>
              js.completed.job.kind match {
                case BatchKind.Quasi => log.warn(Json.obj("fail" -> json), ex)
                case BatchKind.Value => log.error(Json.obj("fail" -> json), ex)
              }
            case Right(_) => log.good(Json.obj("done" -> json))
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
