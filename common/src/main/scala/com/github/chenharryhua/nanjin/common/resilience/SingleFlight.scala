package com.github.chenharryhua.nanjin.common.resilience

import cats.effect.kernel.{Async, Deferred, Ref}
import cats.effect.syntax.monadCancel.given
import cats.syntax.applicative.given
import cats.syntax.applicativeError.given
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import cats.syntax.monadError.given
import cats.syntax.option.{none, given}
import scala.util.control.NoStackTrace

/** A single-flight abstraction. Ensures that for a given effect, at most one computation runs at a time, and
  * all concurrent callers get the same result.
  */

trait SingleFlight[F[_], A] {
  def isBusy: F[Boolean]
  def apply(fa: F[A]): F[A]
  def tryApply(fa: F[A]): F[Option[A]]
}

object SingleFlight {

  // Internal control signal used to unblock followers when the leader fiber is canceled.
  private case object LeaderCancelledException
      extends RuntimeException("SingleFlight leader fiber canceled") with NoStackTrace

  def apply[F[_]: Async, A]: F[SingleFlight[F, A]] =
    Ref.of[F, Option[Deferred[F, Either[Throwable, A]]]](None).map(new Impl[F, A](_))

  final private class Impl[F[_]: Async, A](inFlight: Ref[F, Option[Deferred[F, Either[Throwable, A]]]])
      extends SingleFlight[F, A] {

    // Leader path: publish terminal result (success/failure/cancel) and always clear in-flight state.
    private def runAsLeader(fa: F[A], leader: Deferred[F, Either[Throwable, A]]): F[A] =
      fa.attempt
        .flatTap(leader.complete)
        .onCancel(leader.complete(Left(LeaderCancelledException)).void)
        .guarantee(inFlight.set(None))
        .rethrow

    override val isBusy: F[Boolean] = inFlight.get.map(_.isDefined)

    /** Run the effect, ensuring only one is in flight at a time. */
    override def apply(fa: F[A]): F[A] =
      Deferred[F, Either[Throwable, A]].flatMap { promise =>
        // Atomic admission: first caller becomes leader; others become followers of the same promise.
        inFlight.modify {
          case Some(existing) => Some(existing) -> Right(existing) // follower
          case None           => Some(promise) -> Left(promise) // leader
        }.flatMap {
          case Right(existing) => existing.get.rethrow
          case Left(leader)    => runAsLeader(fa, leader)
        }
      }

    override def tryApply(fa: F[A]): F[Option[A]] =
      Deferred[F, Either[Throwable, A]].flatMap { promise =>
        // Atomic try-admission: returns None immediately when busy, never waits as follower.
        inFlight.modify {
          case Some(existing) => Some(existing) -> Right(existing)
          case None           => Some(promise) -> Left(promise)
        }.flatMap {
          case Right(_)     => none[A].pure[F]
          case Left(leader) => runAsLeader(fa, leader).map(_.some)
        }
      }
  }
}
