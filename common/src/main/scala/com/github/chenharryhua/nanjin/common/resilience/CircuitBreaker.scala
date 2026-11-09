package com.github.chenharryhua.nanjin.common.resilience

import cats.MonadThrow
import cats.effect.kernel.{Async, Outcome, Resource}
import cats.effect.syntax.spawn.given
import cats.syntax.applicative.given
import cats.syntax.applicativeError.given
import cats.syntax.eq.given
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import com.github.chenharryhua.nanjin.common.chrono.{tickStream, Policy, Tick}
import fs2.Stream
import io.circe.{Encoder, Json}

import java.time.ZoneId
import scala.util.control.NoStackTrace

/** A circuit breaker that protects effectful computations from repeated failures.
  *
  * The breaker transitions through Closed, Open, and HalfOpen states. While open, calls are rejected
  * immediately with `RejectedException`.
  */
trait CircuitBreaker[F[_]] {
  def attempt[A](fa: F[A]): F[Either[Throwable, A]]
  def protect[A](fa: F[A]): F[A]
  def state: F[CircuitBreaker.State]
}

object CircuitBreaker {
  def noop[F[_]: MonadThrow]: CircuitBreaker[F] = new CircuitBreaker[F] {
    override def attempt[A](fa: F[A]): F[Either[Throwable, A]] = fa.attempt
    override def protect[A](fa: F[A]): F[A] = fa
    override def state: F[State] = State.Closed(0).pure[F]
  }

  enum State:
    case Closed(failures: Int)
    case HalfOpen
    case Open
  end State

  object State:
    given Encoder[State] =
      case State.Closed(failures) =>
        Json.obj("state" -> Json.fromString("Closed"), "failures" -> Json.fromInt(failures))
      case State.HalfOpen =>
        Json.obj("state" -> Json.fromString("Half-Open"))
      case State.Open =>
        Json.obj("state" -> Json.fromString("Open"))
  end State

  /** `NoStackTrace` already suppresses the trace, and deliberately honours
    * `-Dscala.control.noTraceSuppression=true`, which restores traces when debugging. Overriding
    * `fillInStackTrace` here would defeat that flag for no gain.
    */
  case object RejectedException extends Exception("CircuitBreaker rejected") with NoStackTrace

  final private class Impl[F[_]](max_failures: Int, ticks: Stream[F, Tick])(using F: Async[F]) {

    private enum InternalState:
      case Closed(failures: Int)
      case HalfOpen
      case HalfOpenRunning
      case Open

    /** @param version
      *   bumped on every effective state change. It identifies one particular occupancy of a state, and is
      *   the compare key for a half-open probe: a probe's completion applies only while the `HalfOpenRunning`
      *   it created is still the current one.
      * @param closed_epoch
      *   bumped only when the breaker (re-)enters `Closed` from a non-closed state, so it names one
      *   uninterrupted closed period and is unchanged by failure-count changes within that period. It is the
      *   compare key for a closed-state call: every failure admitted in the same closed period counts, while
      *   a failure left over from an earlier period is discarded.
      */
    private case class MachineState(state: InternalState, version: Long, closed_epoch: Long)

    private val init_closed: InternalState.Closed = InternalState.Closed(0)
    private val init_open: InternalState.Open.type = InternalState.Open

    private def to_public_state(state: InternalState): State = state match {
      case InternalState.Closed(failures) => State.Closed(failures)
      case InternalState.HalfOpen         => State.HalfOpen
      case InternalState.HalfOpenRunning  => State.HalfOpen
      case InternalState.Open             => State.Open
    }

    /** The closed period `next` belongs to: a new one when the breaker re-enters `Closed` from elsewhere,
      * otherwise the current one. A failure-count change inside `Closed` stays in the same period, which is
      * what lets overlapping failures accumulate rather than cancel each other out.
      */
    private def next_closed_epoch(ms: MachineState, next: InternalState): Long =
      (ms.state, next) match {
        case (InternalState.Closed(_), InternalState.Closed(_)) => ms.closed_epoch
        case (_, InternalState.Closed(_))                       => ms.closed_epoch + 1
        case _                                                  => ms.closed_epoch
      }

    private def transition_to(ms: MachineState, next: InternalState): MachineState =
      (ms.state, next) match {
        case (InternalState.Closed(l), InternalState.Closed(r)) if l === r  => ms
        case (InternalState.HalfOpen, InternalState.HalfOpen)               => ms
        case (InternalState.HalfOpenRunning, InternalState.HalfOpenRunning) => ms
        case (InternalState.Open, InternalState.Open)                       => ms
        case _ => MachineState(next, ms.version + 1, next_closed_epoch(ms, next))
      }

    /** How a call was admitted, together with the state facts captured at that moment.
      *
      * These three types and `evolve` below depend only on the breaker's configuration, not on the protected
      * effect, so they live here rather than inside `protect` and are built once for the breaker instead of
      * once per protected call.
      */
    private enum AdmittedFrom:
      case Closed(failures_at_admission: Int, closed_epoch_at_admission: Long)
      case HalfOpen(version_at_admission: Long)

    private enum Decision:
      case Run(from: AdmittedFrom)
      case Reject

    private enum Result:
      case Succeeded
      case Failed
      case Canceled

    /** Pure transition function for post-run outcomes.
      *
      * It uses admission provenance (`from`) to decide whether a completion is still relevant.
      */
    private def evolve(from: AdmittedFrom, result: Result)(ms: MachineState): MachineState =
      ms.state match {
        case InternalState.HalfOpenRunning =>
          from match {
            case AdmittedFrom.HalfOpen(_) =>
              result match {
                case Result.Succeeded => transition_to(ms, init_closed)
                case Result.Failed    => transition_to(ms, init_open)
                case Result.Canceled  => transition_to(ms, InternalState.HalfOpen)
              }
            case AdmittedFrom.Closed(_, _) => ms
          }

        case InternalState.HalfOpen => ms

        case InternalState.Open => ms

        case InternalState.Closed(failures) =>
          from match {
            case AdmittedFrom.HalfOpen(_)                      => ms
            case AdmittedFrom.Closed(failures_at_admission, _) =>
              result match {
                // A success resets the count only when no other call changed it since admission, so a
                // success that was already in flight cannot erase failures recorded after it started.
                case Result.Succeeded if failures === failures_at_admission =>
                  transition_to(ms, init_closed)
                // Every failure admitted in this closed period counts, including ones that overlapped
                // other calls. The caller's `maxFailures` would otherwise be reached only by strictly
                // sequential failures, leaving the breaker closed under concurrent load.
                case Result.Failed =>
                  if (failures + 1 < max_failures)
                    transition_to(ms, InternalState.Closed(failures + 1))
                  else transition_to(ms, init_open)
                case _ => ms
              }
          }
      }

    /** Scheduler is assumed to be non-terminating and non-failing.
      *
      * If it completes, fails, or is canceled, breaker state is left unchanged.
      */
    val state_machine: Resource[F, CircuitBreaker[F]] = for {
      state_ref <- Resource.eval(F.ref[MachineState](MachineState(init_closed, 0L, 0L)))
      _ <- ticks.evalMap { _ =>
        state_ref.update { ms =>
          ms.state match {
            case InternalState.Open => transition_to(ms, InternalState.HalfOpen)
            case _                  => ms
          }
        }
      }.compile.drain.background
    } yield new CircuitBreaker[F] {

      override val state: F[State] = state_ref.get.map(ms => to_public_state(ms.state))

      /** Admit or reject one call. Built once for the breaker: it reads and transitions `state_ref` and
        * depends on nothing specific to the protected effect.
        */
      private val admit: F[Decision] =
        state_ref.modify {
          case ms @ MachineState(InternalState.Open, _, _) =>
            ms -> Decision.Reject
          case ms @ MachineState(InternalState.HalfOpen, _, _) =>
            val next = transition_to(ms, InternalState.HalfOpenRunning)
            next -> Decision.Run(AdmittedFrom.HalfOpen(next.version))
          case ms @ MachineState(InternalState.HalfOpenRunning, _, _) =>
            ms -> Decision.Reject
          case ms @ MachineState(InternalState.Closed(failures), _, closed_epoch) =>
            ms -> Decision.Run(AdmittedFrom.Closed(failures, closed_epoch))
        }

      /** Apply `transform` only if the state version still matches admission time, i.e. the `HalfOpenRunning`
        * created at admission is still the current one.
        *
        * This prevents a stale probe completion from an older fiber, one whose half-open run was already
        * replaced by a cancellation and a later probe, from overwriting newer state.
        */
      private def update_if_current_version(version_at_admission: Long)(
        transform: MachineState => MachineState): F[Unit] =
        state_ref.update { ms =>
          if (ms.version === version_at_admission) transform(ms) else ms
        }

      /** Apply `transform` only if the breaker is still in the same closed period it was admitted in.
        *
        * Unlike the version check, this deliberately tolerates failure-count changes made by overlapping
        * closed-state calls, so concurrent failures all count towards `maxFailures`. It still discards a
        * completion left over from a closed period that has since been interrupted by an open/half-open
        * cycle.
        */
      private def update_if_same_closed_epoch(closed_epoch_at_admission: Long)(
        transform: MachineState => MachineState): F[Unit] =
        state_ref.update { ms =>
          if (ms.closed_epoch === closed_epoch_at_admission) transform(ms) else ms
        }

      /** Record `result` for one call, guarded by the provenance captured at admission: a half-open probe
        * applies only while its own half-open run is current, a closed-state call only while its closed
        * period is current.
        */
      private def record(from: AdmittedFrom, result: Result): F[Unit] = from match {
        case AdmittedFrom.HalfOpen(version_at_admission) =>
          update_if_current_version(version_at_admission)(evolve(from, result))
        case AdmittedFrom.Closed(_, closed_epoch_at_admission) =>
          update_if_same_closed_epoch(closed_epoch_at_admission)(evolve(from, result))
      }

      override def protect[A](fa: F[A]): F[A] =
        admit.flatMap {
          case Decision.Reject => F.raiseError(RejectedException)

          case Decision.Run(from) =>
            F.guaranteeCase(fa) {
              case Outcome.Succeeded(_) => record(from, Result.Succeeded)

              case Outcome.Errored(_) => record(from, Result.Failed)

              case Outcome.Canceled() =>
                from match {
                  case AdmittedFrom.HalfOpen(_) => record(from, Result.Canceled)
                  // A canceled closed-state call never ran to a verdict, so it neither counts as a failure
                  // nor resets the count.
                  case AdmittedFrom.Closed(_, _) => F.unit
                }
            }
        }

      override def attempt[A](fa: F[A]): F[Either[Throwable, A]] = protect(fa).attempt
    }
  }

  /** Create a circuit breaker.
    *
    * `maxFailures` is the number of failures required to transition the breaker from closed to open.
    *
    * For example, a value of `3` opens the breaker on the third failure while in the closed state. Failures
    * count whether they occur sequentially or concurrently: three calls that fail in parallel open the
    * breaker just as three successive failures do. A success resets the count, but only when no other call
    * changed it while that success was running, so a failure overlapping a success is never erased. A
    * canceled call neither counts nor resets. Only failures from the current closed period count: one left in
    * flight from before an open/half-open cycle is discarded.
    *
    * Must be greater than `0`.
    *
    * `policy` defines the scheduling cadence used to transition the breaker from open to half-open. For
    * long-lived breakers, the policy should be non-terminating so periodic probe attempts can continue
    * indefinitely.
    */
  def apply[F[_]: Async](
    zoneId: ZoneId,
    maxFailures: Int,
    f: Policy.type => Policy): Resource[F, CircuitBreaker[F]] = {
    require(maxFailures > 0, s"maxFailures must be positive, but was $maxFailures")
    new Impl[F](maxFailures, tickStream.tickScheduled[F](zoneId, f)).state_machine
  }
}
