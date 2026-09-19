package com.github.chenharryhua.nanjin.common.resilience

import cats.Applicative
import cats.effect.kernel.{Async, Deferred, Ref}
import cats.effect.syntax.monadCancel.given
import cats.syntax.applicative.given
import cats.syntax.applicativeError.given
import cats.syntax.eq.given
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import cats.syntax.option.{none, given}

/** A single-flight abstraction. Ensures that for a given effect, at most one computation runs at a time, and
  * all concurrent callers get the same result.
  *
  * The shared computation runs in a dedicated worker fiber. Canceling one caller only stops that caller from
  * waiting. When the last caller cancels, it cancels the worker and waits for its termination.
  */
trait SingleFlight[F[_], A] {
  def isBusy: F[Boolean]
  def apply(fa: F[A]): F[A]
  def tryApply(fa: F[A]): F[Option[A]]
}

object SingleFlight {
  sealed private trait FlightResult[A]
  private object FlightResult {
    final case class Completed[A](value: Either[Throwable, A]) extends FlightResult[A]
    final case class Retry[A]() extends FlightResult[A]
  }

  final private case class Flight[F[_], A](
    id: Long,
    result: Deferred[F, FlightResult[A]],
    cancel: Deferred[F, Unit],
    waiters: Long)

  def noop[F[_]: Applicative, A]: SingleFlight[F, A] = new SingleFlight[F, A] {
    override def isBusy: F[Boolean] = false.pure[F]
    override def apply(fa: F[A]): F[A] = fa
    override def tryApply(fa: F[A]): F[Option[A]] = fa.map(Some(_))
  }

  def apply[F[_]: Async, A]: F[SingleFlight[F, A]] =
    for {
      in_flight <- Ref.of[F, Option[Flight[F, A]]](None)
      next_id <- Ref.of[F, Long](0L)
    } yield new Impl[F, A](in_flight, next_id)

  final private class Impl[F[_]: Async, A](in_flight: Ref[F, Option[Flight[F, A]]], next_id: Ref[F, Long])
      extends SingleFlight[F, A] {

    private def new_flight: F[Flight[F, A]] =
      next_id.getAndUpdate(_ + 1L).flatMap { id =>
        Deferred[F, FlightResult[A]].flatMap { result =>
          Deferred[F, Unit].map(cancel => Flight(id, result, cancel, 1L))
        }
      }

    private def clear_flight(flight: Flight[F, A]): F[Unit] =
      in_flight.update {
        case Some(current) if current.id === flight.id => None
        case current                                   => current
      }

    private def publish(flight: Flight[F, A], result: FlightResult[A]): F[Unit] =
      clear_flight(flight).flatMap(_ => flight.result.complete(result).void)

    private def run_worker(fa: F[A], flight: Flight[F, A]): F[Unit] =
      Async[F].race(fa.attempt, flight.cancel.get).attempt.flatMap {
        case Right(Left(result)) => publish(flight, FlightResult.Completed(result))
        case Right(Right(_))     => publish(flight, FlightResult.Retry())
        case Left(error)         => publish(flight, FlightResult.Completed(Left(error)))
      }

    private def cancel_worker(flight: Flight[F, A]): F[Unit] =
      flight.cancel.complete(()).flatMap(_ =>
        flight.result.get.flatMap {
          case FlightResult.Completed(Left(error)) => Async[F].raiseError(error)
          case _                                   => Applicative[F].unit
        })

    private def remove_waiter(flight: Flight[F, A]): F[Unit] =
      in_flight.modify {
        case Some(current) if current.id === flight.id && current.waiters > 0L =>
          val remaining = current.waiters - 1L
          val updated = current.copy(waiters = remaining)
          val cancel = if (remaining === 0L) cancel_worker(current) else Applicative[F].unit
          Some(updated) -> cancel
        case current =>
          current -> Applicative[F].unit
      }.flatMap(identity)

    private def await_result(flight: Flight[F, A], fa: F[A]): F[A] =
      flight.result.get
        .flatMap {
          case FlightResult.Completed(result) => result.fold(Async[F].raiseError, Async[F].pure)
          case FlightResult.Retry()           => apply(fa)
        }
        .onCancel(remove_waiter(flight))

    private def start_worker(fa: F[A], flight: Flight[F, A]): F[Unit] =
      Async[F].start(run_worker(fa, flight)).attempt.flatMap {
        case Right(_)    => Applicative[F].unit
        case Left(error) =>
          publish(flight, FlightResult.Completed(Left(error))).flatMap(_ => Async[F].raiseError(error))
      }

    private def run(fa: F[A], wait_if_busy: Boolean): F[Option[A]] =
      new_flight.flatMap { candidate =>
        Async[F].uncancelable { poll =>
          in_flight.modify {
            case Some(current) if wait_if_busy =>
              Some(current.copy(waiters = current.waiters + 1L)) -> Some(Right(current))
            case Some(current) =>
              Some(current) -> None
            case None =>
              Some(candidate) -> Some(Left(candidate))
          }.flatMap {
            case Some(Left(leader)) =>
              start_worker(fa, leader).flatMap(_ => poll(await_result(leader, fa)).map(_.some))
            case Some(Right(follower)) =>
              poll(await_result(follower, fa)).map(_.some)
            case None =>
              none[A].pure[F]
          }
        }
      }

    override val isBusy: F[Boolean] = in_flight.get.map(_.isDefined)

    override def apply(fa: F[A]): F[A] =
      run(fa, wait_if_busy = true).flatMap(
        _.fold(Async[F].raiseError[A](new IllegalStateException("unreachable")))(Async[F].pure))

    override def tryApply(fa: F[A]): F[Option[A]] =
      run(fa, wait_if_busy = false)
  }
}
