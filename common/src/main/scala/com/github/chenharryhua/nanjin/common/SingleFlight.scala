package com.github.chenharryhua.nanjin.common

import cats.effect.implicits.monadCancelOps_
import cats.effect.kernel.{Async, Deferred, Ref}
import cats.implicits.{
  catsSyntaxApplicativeError,
  catsSyntaxApplicativeId,
  catsSyntaxMonadErrorRethrow,
  catsSyntaxOptionId,
  none,
  toFlatMapOps,
  toFunctorOps
}

/** A reusable single-flight abstraction. Ensures that for a given effect, at most one computation runs at a
  * time, and all concurrent callers get the same result.
  */
final class SingleFlight[F[_]: Async, A] private (
  inFlight: Ref[F, Option[Deferred[F, Either[Throwable, A]]]]) {

  val isBusy: F[Boolean] = inFlight.get.map(_.isDefined)

  /** Run the effect, ensuring only one is in flight at a time. */
  def apply(fa: F[A]): F[A] =
    Deferred[F, Either[Throwable, A]].flatMap { waiter =>
      inFlight.modify {
        case Some(existing) => Some(existing) -> Right(existing) // follower
        case None           => Some(waiter) -> Left(waiter) // we are the leader
      }.flatMap {
        case Right(existing) => existing.get.rethrow
        case Left(leader)    =>
          fa.attempt.flatTap(leader.complete).guarantee(inFlight.set(None)).rethrow
      }
    }

  def tryApply(fa: F[A]): F[Option[A]] = isBusy.flatMap {
    case true  => none[A].pure[F]
    case false => apply(fa).map(_.some)
  }
}

object SingleFlight {
  def apply[F[_]: Async, A]: F[SingleFlight[F, A]] =
    Ref.of[F, Option[Deferred[F, Either[Throwable, A]]]](None).map(new SingleFlight[F, A](_))
}
