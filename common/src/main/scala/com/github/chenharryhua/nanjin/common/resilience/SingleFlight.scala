package com.github.chenharryhua.nanjin.common.resilience

import cats.Applicative
import cats.effect.kernel.{Async, Deferred, Poll, Ref}
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
      *
      * Named for what a waiter does with it. `Retry` would collide with `resilience.Retry`, an unrelated type
      * in the same package.
      */
    final case class Resubmit[A]() extends FlightResult[A]
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

  /** A caller that has been admitted to a flight, and the role it plays in it.
    *
    * A caller that is turned away is not represented here: `tryApply` reports that as `None`, so the two
    * cases below are the only ways to be admitted and every admitted caller ends up awaiting a result.
    */
  sealed private trait Admission[F[_], A]
  private object Admission {

    /** No flight existed, so this caller created `flight` and is responsible for starting its worker. */
    final case class Leader[F[_], A](flight: Flight[F, A]) extends Admission[F, A]

    /** A flight was already in progress; this caller joined it (as a counted waiter) and only awaits its
      * result.
      */
    final case class Follower[F[_], A](flight: Flight[F, A]) extends Admission[F, A]
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
        case Right(Right(_))     => publish(flight, FlightResult.Resubmit())
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
          case FlightResult.Resubmit()        => apply(fa)
        }
        .onCancel(remove_waiter(flight))

    private def start_worker(fa: F[A], flight: Flight[F, A]): F[Unit] =
      F.start(run_worker(fa, flight)).attempt.flatMap {
        case Right(_)    => F.unit
        case Left(error) =>
          publish(flight, FlightResult.Completed(Left(error))).flatMap(_ => F.raiseError(error))
      }

    /** Join the flight already in progress as a counted waiter, or report that there is none.
      *
      * The observation and the waiter increment are one atomic `modify`, so a flight cannot be torn down
      * between being seen and being joined.
      */
    private def join_in_flight: F[Option[Admission[F, A]]] =
      in_flight.modify {
        case Some(current) =>
          Some(current.copy(waiters = current.waiters + 1L)) -> Some(Admission.Follower(current))
        case None => None -> none[Admission[F, A]]
      }

    /** Install a new flight and make this caller its leader. Callers hold `initialization_lock`. */
    private def become_leader: F[Admission[F, A]] =
      new_flight.flatMap(flight => in_flight.set(Some(flight)).as(Admission.Leader(flight)))

    /** Admission for a caller willing to wait. Such a caller is always admitted: it joins the flight in
      * progress or creates one, which is why `apply` can return `F[A]` rather than an `Option`.
      */
    private def join_or_create: F[Admission[F, A]] =
      join_in_flight.flatMap {
        case Some(admission) => admission.pure[F]
        // Re-check under the lock: another caller may have installed a flight since the first look.
        case None =>
          initialization_lock.lock.surround(join_in_flight.flatMap(_.fold(become_leader)(_.pure)))
      }

    /** Admission for a caller unwilling to wait, yielding `None` when a flight is already in progress. Unlike
      * `join_in_flight` it never increments the waiter count, because such a caller does not wait.
      */
    private def create_if_idle: F[Option[Admission[F, A]]] =
      in_flight.get.flatMap {
        case Some(_) => none[Admission[F, A]].pure[F]
        case None    =>
          initialization_lock.lock.surround(in_flight.get.flatMap {
            case Some(_) => none[Admission[F, A]].pure[F]
            case None    => become_leader.map(_.some)
          })
      }

    private def proceed(fa: F[A], poll: Poll[F])(admission: Admission[F, A]): F[A] =
      admission match {
        case Admission.Leader(flight) =>
          start_worker(fa, flight).flatMap(_ => poll(await_result(flight, fa)))
        case Admission.Follower(flight) =>
          poll(await_result(flight, fa))
      }

    override def apply(fa: F[A]): F[A] =
      F.uncancelable(poll => join_or_create.flatMap(proceed(fa, poll)))

    override def tryApply(fa: F[A]): F[Option[A]] =
      F.uncancelable { poll =>
        create_if_idle.flatMap {
          case Some(admission) => proceed(fa, poll)(admission).map(_.some)
          case None            => none[A].pure[F]
        }
      }
  }
}
