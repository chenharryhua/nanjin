package com.github.chenharryhua.nanjin.guard.batch

import cats.effect.MonadCancelThrow
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
sealed private trait JobHook[F[_], A]:
  def kickoff: Job => F[Unit]
  def canceled: Job => F[Unit]
  def completed: JobState[A] => F[Unit]
end JobHook

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
  final case class Bridge[F[_], A] private[JobHook] (
    completed: JobState[A] => F[Unit],
    canceled: Job => F[Unit],
    kickoff: Job => F[Unit]
  ) extends JobHook[F, A] with Subscriber[F, A] {

    /** Replace the completion callback. */
    override def onComplete(f: JobState[A] => F[Unit]): Bridge[F, A] = copy(completed = f)

    /** Replace the cancellation callback. */
    override def onCancel(f: Job => F[Unit]): Bridge[F, A] = copy(canceled = f)

    /** Replace the kickoff callback. */
    override def onKickoff(f: Job => F[Unit]): Bridge[F, A] = copy(kickoff = f)

    /** Adapt completion payloads from `B` to this hook's `A`. */
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

  /** Create a hook that ignores all lifecycle notifications. */
  def noop[F[_], A](using F: Applicative[F]): Bridge[F, A] =
    new Bridge[F, A](
      completed = _ => F.unit,
      canceled = _ => F.unit,
      kickoff = _ => F.unit
    )

  /** Create logging-based hook builders. */
  def apply[F[_]](logger: Log[F]): ByLogger[F] = ByLogger[F](logger)

  /*
   * Bridge by logger
   */

  final class ByLogger[F[_]](log: Log[F]) {

    /** Log lifecycle events using a custom JSON representation. */
    def universal[A](f: JobState[A] => Json): Bridge[F, A] =
      Bridge[F, A](
        completed = { (js: JobState[A]) =>
          val json: Json = f(js)
          js.result match {
            case Left(ex) =>
              js.completed.job.kind match {
                case BatchKind.Quasi => log.warn(Json.obj(SeverityNonFatal -> json), ex)
                case BatchKind.Value => log.error(Json.obj(SeverityCritical -> json), ex)
              }
            case Right(_) => log.good(Json.obj("done" -> json))
          }
        },
        canceled = (bj: Job) => log.warn(Json.obj("canceled" -> bj.asJson)),
        kickoff = (bj: Job) => log.info(Json.obj("kickoff" -> bj.asJson))
      )

    /** Log lifecycle events using the value's Circe encoder. */
    def standard[A: Encoder]: Bridge[F, A] = universal[A](_.asJson)

    /** Log JSON job states directly. */
    def json: Bridge[F, Json] = standard[Json]
  }

  given [F[_], A](using F: MonadCancelThrow[F]): Monoid[Bridge[F, A]] =
    new Monoid[Bridge[F, A]] {

      override val empty: Bridge[F, A] = noop[F, A]

      override def combine(x: Bridge[F, A], y: Bridge[F, A]): Bridge[F, A] =
        Bridge[F, A](
          completed = js => F.uncancelable(_ => x.completed(js) >> y.completed(js)),
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
