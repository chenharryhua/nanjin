package com.github.chenharryhua.nanjin.common.resilience

import cats.effect.kernel.{Async, Outcome, Resource}
import cats.effect.syntax.spawn.given
import cats.syntax.applicativeError.given
import cats.syntax.eq.given
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import com.github.chenharryhua.nanjin.common.chrono.{tickStream, Policy, Tick}
import fs2.Stream
import io.circe.{Encoder, Json}

import java.time.ZoneId
import scala.util.control.NoStackTrace

trait CircuitBreaker[F[_]] {
  def attempt[A](fa: F[A]): F[Either[Throwable, A]]
  def protect[A](fa: F[A]): F[A]
  def getState: F[CircuitBreaker.State]
}

object CircuitBreaker {
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

    private case class MachineState(state: InternalState, version: Long)

    private val initClosed: InternalState.Closed = InternalState.Closed(0)
    private val initOpen: InternalState.Open.type = InternalState.Open

    private def toPublicState(state: InternalState): State = state match {
      case InternalState.Closed(failures) => State.Closed(failures)
      case InternalState.HalfOpen         => State.HalfOpen
      case InternalState.HalfOpenRunning  => State.HalfOpen
      case InternalState.Open             => State.Open
    }

    private def transitionTo(ms: MachineState, next: InternalState): MachineState =
      (ms.state, next) match {
        case (InternalState.Closed(l), InternalState.Closed(r)) if l === r  => ms
        case (InternalState.HalfOpen, InternalState.HalfOpen)               => ms
        case (InternalState.HalfOpenRunning, InternalState.HalfOpenRunning) => ms
        case (InternalState.Open, InternalState.Open)                       => ms
        case _ => MachineState(next, ms.version + 1)
      }

    /** Scheduler is assumed to be non-terminating and non-failing.
      *
      * If it completes, fails, or is canceled, breaker state is left unchanged.
      */
    val stateMachine: Resource[F, CircuitBreaker[F]] = for {
      state <- Resource.eval(F.ref[MachineState](MachineState(initClosed, 0L)))
      _ <- ticks.evalMap { _ =>
        state.update { ms =>
          ms.state match {
            case InternalState.Open => transitionTo(ms, InternalState.HalfOpen)
            case _                  => ms
          }
        }
      }.compile.drain.background
    } yield new CircuitBreaker[F] {

      override val getState: F[State] = state.get.map(ms => toPublicState(ms.state))

      override def protect[A](fa: F[A]): F[A] = {

        enum AdmittedFrom:
          case Closed(failuresAtAdmission: Int, versionAtAdmission: Long)
          case HalfOpen(versionAtAdmission: Long)

        enum Decision:
          case Run(from: AdmittedFrom)
          case Reject

        enum Result:
          case Succeeded
          case Failed
          case Canceled

        val admit: F[Decision] =
          state.modify {
            case ms @ MachineState(InternalState.Open, _) =>
              ms -> Decision.Reject
            case ms @ MachineState(InternalState.HalfOpen, _) =>
              val next = transitionTo(ms, InternalState.HalfOpenRunning)
              next -> Decision.Run(AdmittedFrom.HalfOpen(next.version))
            case ms @ MachineState(InternalState.HalfOpenRunning, _) =>
              ms -> Decision.Reject
            case ms @ MachineState(InternalState.Closed(failures), version) =>
              ms -> Decision.Run(AdmittedFrom.Closed(failures, version))
          }

        /** Apply an update only if the state version still matches admission time.
          *
          * This prevents stale completions from older fibers from overwriting newer state.
          */
        def updateIfCurrent(versionAtAdmission: Long)(evolve: MachineState => MachineState): F[Unit] =
          state.update { ms =>
            if (ms.version === versionAtAdmission) evolve(ms) else ms
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
                    // Guard against stale success/failure: only modify count when admission count still matches.
                    case Result.Succeeded if failures === failuresAtAdmission =>
                      transitionTo(ms, initClosed)
                    case Result.Failed if failures === failuresAtAdmission =>
                      if (failures < maxFailures)
                        transitionTo(ms, InternalState.Closed(failures + 1))
                      else transitionTo(ms, initOpen)
                    case _ => ms
                  }
              }
          }

        admit.flatMap {
          case Decision.Reject => F.raiseError(RejectedException)

          case Decision.Run(from) =>
            F.guaranteeCase(fa) {
              case Outcome.Succeeded(_) =>
                from match {
                  case AdmittedFrom.HalfOpen(versionAtAdmission) =>
                    updateIfCurrent(versionAtAdmission)(evolve(from, Result.Succeeded))

                  case AdmittedFrom.Closed(failuresAtAdmission, versionAtAdmission) =>
                    updateIfCurrent(versionAtAdmission)(evolve(from, Result.Succeeded))
                }

              case Outcome.Errored(_) =>
                from match {
                  case AdmittedFrom.HalfOpen(versionAtAdmission) =>
                    updateIfCurrent(versionAtAdmission)(evolve(from, Result.Failed))

                  case AdmittedFrom.Closed(failuresAtAdmission, versionAtAdmission) =>
                    updateIfCurrent(versionAtAdmission)(evolve(from, Result.Failed))
                }

              case Outcome.Canceled() =>
                from match {
                  case AdmittedFrom.HalfOpen(versionAtAdmission) =>
                    updateIfCurrent(versionAtAdmission)(evolve(from, Result.Canceled))

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
    * `maxFailures` is the number of consecutive failures required to transition the breaker from closed to
    * open.
    *
    * For example, a value of `3` opens the breaker after the third consecutive failure while in the closed
    * state.
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
    policy: Policy.type => Policy): Resource[F, CircuitBreaker[F]] = {
    require(maxFailures > 0, s"maxFailures should be greater than 0, but got $maxFailures")
    new Impl[F](maxFailures, tickStream.tickScheduled[F](zoneId, policy)).stateMachine
  }
}
