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

  case object RejectedException extends Exception("CircuitBreaker rejected") with NoStackTrace {
    override def fillInStackTrace(): Throwable = this
  }

  final private class Impl[F[_]](maxFailures: Int, ticks: Stream[F, Tick])(using F: Async[F]) {

    private enum InternalState:
      case Closed(failures: Int)
      case HalfOpen
      case HalfOpenRunning
      case Open

    /** @param version
      *   bumped on every effective state change. It identifies one particular occupancy of a state, and is
      *   the compare key for a half-open probe: a probe's completion applies only while the `HalfOpenRunning`
      *   it created is still the current one.
      * @param closedEpoch
      *   bumped only when the breaker (re-)enters `Closed` from a non-closed state, so it names one
      *   uninterrupted closed period and is unchanged by failure-count changes within that period. It is the
      *   compare key for a closed-state call: every failure admitted in the same closed period counts, while
      *   a failure left over from an earlier period is discarded.
      */
    private case class MachineState(state: InternalState, version: Long, closedEpoch: Long)

    private val initClosed: InternalState.Closed = InternalState.Closed(0)
    private val initOpen: InternalState.Open.type = InternalState.Open

    private def toPublicState(state: InternalState): State = state match {
      case InternalState.Closed(failures) => State.Closed(failures)
      case InternalState.HalfOpen         => State.HalfOpen
      case InternalState.HalfOpenRunning  => State.HalfOpen
      case InternalState.Open             => State.Open
    }

    /** The closed period `next` belongs to: a new one when the breaker re-enters `Closed` from elsewhere,
      * otherwise the current one. A failure-count change inside `Closed` stays in the same period, which is
      * what lets overlapping failures accumulate rather than cancel each other out.
      */
    private def nextClosedEpoch(ms: MachineState, next: InternalState): Long =
      (ms.state, next) match {
        case (InternalState.Closed(_), InternalState.Closed(_)) => ms.closedEpoch
        case (_, InternalState.Closed(_))                       => ms.closedEpoch + 1
        case _                                                  => ms.closedEpoch
      }

    private def transitionTo(ms: MachineState, next: InternalState): MachineState =
      (ms.state, next) match {
        case (InternalState.Closed(l), InternalState.Closed(r)) if l === r  => ms
        case (InternalState.HalfOpen, InternalState.HalfOpen)               => ms
        case (InternalState.HalfOpenRunning, InternalState.HalfOpenRunning) => ms
        case (InternalState.Open, InternalState.Open)                       => ms
        case _ => MachineState(next, ms.version + 1, nextClosedEpoch(ms, next))
      }

    /** Scheduler is assumed to be non-terminating and non-failing.
      *
      * If it completes, fails, or is canceled, breaker state is left unchanged.
      */
    val stateMachine: Resource[F, CircuitBreaker[F]] = for {
      stateRef <- Resource.eval(F.ref[MachineState](MachineState(initClosed, 0L, 0L)))
      _ <- ticks.evalMap { _ =>
        stateRef.update { ms =>
          ms.state match {
            case InternalState.Open => transitionTo(ms, InternalState.HalfOpen)
            case _                  => ms
          }
        }
      }.compile.drain.background
    } yield new CircuitBreaker[F] {

      override val state: F[State] = stateRef.get.map(ms => toPublicState(ms.state))

      override def protect[A](fa: F[A]): F[A] = {

        enum AdmittedFrom:
          case Closed(failuresAtAdmission: Int, closedEpochAtAdmission: Long)
          case HalfOpen(versionAtAdmission: Long)

        enum Decision:
          case Run(from: AdmittedFrom)
          case Reject

        enum Result:
          case Succeeded
          case Failed
          case Canceled

        val admit: F[Decision] =
          stateRef.modify {
            case ms @ MachineState(InternalState.Open, _, _) =>
              ms -> Decision.Reject
            case ms @ MachineState(InternalState.HalfOpen, _, _) =>
              val next = transitionTo(ms, InternalState.HalfOpenRunning)
              next -> Decision.Run(AdmittedFrom.HalfOpen(next.version))
            case ms @ MachineState(InternalState.HalfOpenRunning, _, _) =>
              ms -> Decision.Reject
            case ms @ MachineState(InternalState.Closed(failures), _, closedEpoch) =>
              ms -> Decision.Run(AdmittedFrom.Closed(failures, closedEpoch))
          }

        /** Apply an update only if the state version still matches admission time, i.e. the `HalfOpenRunning`
          * created at admission is still the current one.
          *
          * This prevents a stale probe completion from an older fiber — one whose half-open run was already
          * replaced by a cancellation and a later probe — from overwriting newer state.
          */
        def updateIfCurrentVersion(versionAtAdmission: Long)(evolve: MachineState => MachineState): F[Unit] =
          stateRef.update { ms =>
            if (ms.version === versionAtAdmission) evolve(ms) else ms
          }

        /** Apply an update only if the breaker is still in the same closed period it was admitted in.
          *
          * Unlike the version check, this deliberately tolerates failure-count changes made by overlapping
          * closed-state calls, so concurrent failures all count towards `maxFailures`. It still discards a
          * completion left over from a closed period that has since been interrupted by an open/half-open
          * cycle.
          */
        def updateIfSameClosedEpoch(closedEpochAtAdmission: Long)(
          evolve: MachineState => MachineState): F[Unit] =
          stateRef.update { ms =>
            if (ms.closedEpoch === closedEpochAtAdmission) evolve(ms) else ms
          }

        /** Pure transition function for post-run outcomes.
          *
          * It uses admission provenance (`from`) to decide whether a completion is still relevant.
          */
        def evolve(from: AdmittedFrom, result: Result)(ms: MachineState): MachineState =
          ms.state match {
            case InternalState.HalfOpenRunning =>
              from match {
                case AdmittedFrom.HalfOpen(_) =>
                  result match {
                    case Result.Succeeded => transitionTo(ms, initClosed)
                    case Result.Failed    => transitionTo(ms, initOpen)
                    case Result.Canceled  => transitionTo(ms, InternalState.HalfOpen)
                  }
                case AdmittedFrom.Closed(_, _) => ms
              }

            case InternalState.HalfOpen => ms

            case InternalState.Open => ms

            case InternalState.Closed(failures) =>
              from match {
                case AdmittedFrom.HalfOpen(_)                    => ms
                case AdmittedFrom.Closed(failuresAtAdmission, _) =>
                  result match {
                    // A success resets the count only when no other call changed it since admission, so a
                    // success that was already in flight cannot erase failures recorded after it started.
                    case Result.Succeeded if failures === failuresAtAdmission =>
                      transitionTo(ms, initClosed)
                    // Every failure admitted in this closed period counts, including ones that overlapped
                    // other calls. The caller's `maxFailures` would otherwise be reached only by strictly
                    // sequential failures, leaving the breaker closed under concurrent load.
                    case Result.Failed =>
                      if (failures + 1 < maxFailures)
                        transitionTo(ms, InternalState.Closed(failures + 1))
                      else transitionTo(ms, initOpen)
                    case _ => ms
                  }
              }
          }

        /** Record `result` for this call, guarded by the provenance captured at admission: a half-open probe
          * applies only while its own half-open run is current, a closed-state call only while its closed
          * period is current.
          */
        def record(from: AdmittedFrom, result: Result): F[Unit] = from match {
          case AdmittedFrom.HalfOpen(versionAtAdmission) =>
            updateIfCurrentVersion(versionAtAdmission)(evolve(from, result))
          case AdmittedFrom.Closed(_, closedEpochAtAdmission) =>
            updateIfSameClosedEpoch(closedEpochAtAdmission)(evolve(from, result))
        }

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
    new Impl[F](maxFailures, tickStream.tickScheduled[F](zoneId, f)).stateMachine
  }
}
