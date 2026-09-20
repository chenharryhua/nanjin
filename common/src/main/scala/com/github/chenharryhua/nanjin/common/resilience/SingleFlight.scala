package com.github.chenharryhua.nanjin.common.resilience

import cats.Applicative
import cats.effect.kernel.{Async, Deferred, Ref}
import cats.effect.std.Mutex
import cats.effect.syntax.monadCancel.given
import cats.syntax.applicative.given
import cats.syntax.applicativeError.given
import cats.syntax.eq.given
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import cats.syntax.option.{none, given}

/** A single-flight abstraction that treats each `SingleFlight` instance as one implicit key.
  *
  * At most one submitted effect runs at a time. Concurrent callers join that flight and normally receive its
  * result without evaluating their own effects. If a joined flight is canceled after losing its previous
  * waiters, remaining callers resubmit their effects to a replacement flight. Callers sharing an instance
  * must therefore submit logically equivalent operations.
  *
  * The shared computation runs in a dedicated worker fiber. Canceling one caller only stops that caller from
  * waiting. When the last caller cancels, it cancels the worker and waits for its termination; if the worker
  * remains indefinitely uncancelable, that cancellation remains pending. Failures raised by the worker's
  * cancellation finalizers follow the effect runtime's reporting semantics; they are not returned as a normal
  * result to the canceled caller.
  */
trait SingleFlight[F[_], A] {

  /** Submit an operation, or join the operation already in flight.
    *
    * When an active flight completes, a follower's `fa` is not evaluated and the follower receives that
    * flight's result. A caller that joins a flight already being canceled waits for its teardown and
    * resubmits `fa` to a replacement flight. Consequently, every `fa` submitted to the same instance must
    * represent the same logical operation.
    */
  def apply(fa: F[A]): F[A]

  /** Run `fa` only when no operation is in flight; otherwise return `None` without evaluating `fa`. */
  def tryApply(fa: F[A]): F[Option[A]]
}

object SingleFlight {

  /** The outcome a worker publishes into `Flight.result` for its waiters to observe. */
  sealed private trait FlightResult[A]
  private object FlightResult {

    /** The flight ran to completion; `value` is the effect's success or failure, delivered to every waiter.
      */
    final case class Completed[A](value: Either[Throwable, A]) extends FlightResult[A]

    /** The flight was canceled (its last waiter left, so the worker was canceled) before producing a value.
      * Waiters that observe this resubmit their effect to a fresh flight rather than fail.
      */
    final case class Retry[A]() extends FlightResult[A]
  }

  /** One in-flight computation shared by its callers, held in the `in_flight` slot while it runs.
    *
    * @param id
    *   a unique, monotonically assigned identifier. It is the compare key: state changes (`clear_flight`,
    *   `remove_waiter`) act only when the slot still holds this same flight, so a later flight that reused
    *   the slot is never mutated by a stale caller.
    * @param result
    *   completed once by the worker with the flight's `FlightResult`; every waiter reads its outcome here.
    * @param cancel
    *   completed once to signal the worker to stop. The worker races the effect against `cancel`, and the
    *   last waiter to leave completes it to tear the flight down.
    * @param waiters
    *   the number of callers currently awaiting this flight, incremented as callers join and decremented as
    *   they cancel. When it reaches zero the worker is canceled.
    */
  final private case class Flight[F[_], A](
    id: Long,
    result: Deferred[F, FlightResult[A]],
    cancel: Deferred[F, Unit],
    waiters: Long)

  /** The result of a caller trying to enter the single flight, deciding what that caller does next. */
  sealed private trait Admission[F[_], A]
  private object Admission {

    /** No flight existed, so this caller created `flight` and is responsible for starting its worker. */
    final case class Leader[F[_], A](flight: Flight[F, A]) extends Admission[F, A]

    /** A flight was already in progress; this caller joined it (as a counted waiter) and only awaits its
      * result.
      */
    final case class Follower[F[_], A](flight: Flight[F, A]) extends Admission[F, A]

    /** A flight was in progress but the caller opted not to wait (`tryApply`); it returns `None` without
      * running its effect.
      */
    final case class Busy[F[_], A]() extends Admission[F, A]
  }

  def noop[F[_]: Applicative, A]: SingleFlight[F, A] = new SingleFlight[F, A] {
    override def apply(fa: F[A]): F[A] = fa
    override def tryApply(fa: F[A]): F[Option[A]] = fa.map(Some(_))
  }

  def apply[F[_]: Async, A]: F[SingleFlight[F, A]] =
    for {
      in_flight <- Ref.of[F, Option[Flight[F, A]]](None)
      next_id <- Ref.of[F, Long](0L)
      initialization_lock <- Mutex[F]
    } yield new Impl[F, A](in_flight, next_id, initialization_lock)

  final private class Impl[F[_], A](
    in_flight: Ref[F, Option[Flight[F, A]]],
    next_id: Ref[F, Long],
    initialization_lock: Mutex[F])(using F: Async[F])
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
      F.race(fa.attempt, flight.cancel.get).attempt.flatMap {
        case Right(Left(result)) => publish(flight, FlightResult.Completed(result))
        case Right(Right(_))     => publish(flight, FlightResult.Retry())
        case Left(error)         => publish(flight, FlightResult.Completed(Left(error)))
      }

    private def cancel_worker(flight: Flight[F, A]): F[Unit] =
      flight.cancel.complete(()).flatMap(_ =>
        flight.result.get.flatMap {
          case FlightResult.Completed(Left(error)) => F.raiseError(error)
          case _                                   => F.unit
        })

    private def remove_waiter(flight: Flight[F, A]): F[Unit] =
      in_flight.modify {
        case Some(current) if current.id === flight.id && current.waiters > 0L =>
          val remaining = current.waiters - 1L
          val updated = current.copy(waiters = remaining)
          val cancel = if (remaining === 0L) cancel_worker(current) else F.unit
          Some(updated) -> cancel
        case current =>
          current -> F.unit
      }.flatMap(identity)

    private def await_result(flight: Flight[F, A], fa: F[A]): F[A] =
      flight.result.get
        .flatMap {
          case FlightResult.Completed(result) => result.fold(F.raiseError, F.pure)
          case FlightResult.Retry()           => apply(fa)
        }
        .onCancel(remove_waiter(flight))

    private def start_worker(fa: F[A], flight: Flight[F, A]): F[Unit] =
      F.start(run_worker(fa, flight)).attempt.flatMap {
        case Right(_)    => F.unit
        case Left(error) =>
          publish(flight, FlightResult.Completed(Left(error))).flatMap(_ => F.raiseError(error))
      }

    private def admit_existing(wait_if_busy: Boolean): F[Option[Admission[F, A]]] =
      in_flight.modify {
        case Some(current) if wait_if_busy =>
          Some(current.copy(waiters = current.waiters + 1L)) -> Some(Admission.Follower(current))
        case current @ Some(_) => current -> Some(Admission.Busy())
        case None              => None -> None
      }

    private def initialize(wait_if_busy: Boolean): F[Admission[F, A]] =
      initialization_lock.lock.surround(admit_existing(wait_if_busy).flatMap {
        case Some(admission) => F.pure(admission)
        case None            =>
          new_flight.flatMap { flight =>
            in_flight.set(Some(flight)).flatMap(_ => Admission.Leader(flight).pure)
          }
      })

    private def proceed(fa: F[A], poll: cats.effect.kernel.Poll[F]): Admission[F, A] => F[Option[A]] = {
      case Admission.Leader(flight) =>
        start_worker(fa, flight).flatMap(_ => poll(await_result(flight, fa)).map(_.some))
      case Admission.Follower(flight) =>
        poll(await_result(flight, fa)).map(_.some)
      case Admission.Busy() => none[A].pure[F]
    }

    private def run(fa: F[A], wait_if_busy: Boolean): F[Option[A]] =
      Async[F].uncancelable { poll =>
        admit_existing(wait_if_busy)
          .flatMap(_.fold(initialize(wait_if_busy))(_.pure))
          .flatMap(proceed(fa, poll))
      }

    override def apply(fa: F[A]): F[A] =
      run(fa, wait_if_busy = true).flatMap(
        _.fold(F.raiseError[A](new IllegalStateException("unreachable")))(_.pure))

    override def tryApply(fa: F[A]): F[Option[A]] =
      run(fa, wait_if_busy = false)
  }
}
