package com.github.chenharryhua.nanjin.common.resilience

import cats.Endo
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
    case Broken
  end State

  object State:
    given Encoder[State] =
      case State.Closed(failures) =>
        Json.obj("state" -> Json.fromString("Closed"), "failures" -> Json.fromInt(failures))
      case State.HalfOpen =>
        Json.obj("state" -> Json.fromString("Half-Open"))
      case State.Open =>
        Json.obj("state" -> Json.fromString("Open"))
      case State.Broken =>
        Json.obj("state" -> Json.fromString("Broken"))
  end State

  final class RejectedException private[CircuitBreaker] (val state: State)
      extends Exception(s"CircuitBreaker rejected in state=$state") with NoStackTrace {
    override def fillInStackTrace(): Throwable = this
  }

  private case object BrokenException extends Exception("CircuitBreaker is broken") with NoStackTrace {
    override def fillInStackTrace(): Throwable = this
  }

  private def rejectedException(state: State): Throwable = new RejectedException(state)

  private[resilience] def rejectionForState(state: State): Throwable = state match {
    case State.Broken => BrokenException
    case other        => rejectedException(other)
  }

  final private class Impl[F[_]](maxFailures: Int, ticks: Stream[F, Tick])(using F: Async[F]) {

    private enum InternalState:
      case Closed(failures: Int)
      case HalfOpen
      case HalfOpenRunning
      case Open
      case Broken

    private case class MachineState(state: InternalState, version: Long)

    private val initClosed: InternalState.Closed = InternalState.Closed(0)
    private val initOpen: InternalState.Open.type = InternalState.Open

    private def toPublicState(state: InternalState): State = state match {
      case InternalState.Closed(failures) => State.Closed(failures)
      case InternalState.HalfOpen         => State.HalfOpen
      case InternalState.HalfOpenRunning  => State.HalfOpen
      case InternalState.Open             => State.Open
      case InternalState.Broken           => State.Broken
    }

    private def transitionBroken(ms: MachineState): MachineState =
      transitionTo(ms, InternalState.Broken)

    private def transitionTo(ms: MachineState, next: InternalState): MachineState =
      next match {
        case InternalState.Broken =>
          ms.state match {
            case InternalState.Broken => ms // Broken is terminal.
            case _                    => MachineState(next, ms.version + 1)
          }
        case _ =>
          (ms.state, next) match {
            case (InternalState.Closed(l), InternalState.Closed(r)) if l === r  => ms
            case (InternalState.HalfOpen, InternalState.HalfOpen)               => ms
            case (InternalState.HalfOpenRunning, InternalState.HalfOpenRunning) => ms
            case (InternalState.Open, InternalState.Open)                       => ms
            case _ => MachineState(next, ms.version + 1)
          }
      }

    /** Scheduler lifecycle is strict: if tick stream finishes or fails, breaker is terminally broken.
      *
      *   - Success: scheduler stopped, breaker becomes Broken
      *   - Error: scheduler failed, breaker becomes Broken
      *   - Cancel: resource release, no state transition
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
      }.onFinalizeCase {
        case Resource.ExitCase.Succeeded  => state.update(ms => transitionBroken(ms))
        case Resource.ExitCase.Errored(_) => state.update(ms => transitionBroken(ms))
        case Resource.ExitCase.Canceled   => F.unit
      }.compile.drain.background
    } yield new CircuitBreaker[F] {

      override val getState: F[State] = state.get.map(ms => toPublicState(ms.state))

      private def rejectionError[A](ms: MachineState): F[A] = ms.state match {
        case InternalState.Broken => F.raiseError(BrokenException)
        case other                => F.raiseError(rejectionForState(toPublicState(other)))
      }

      private val ensureNotBroken: F[Unit] =
        state.get.flatMap {
          case ms @ MachineState(InternalState.Broken, _) => rejectionError(ms)
          case _                                          => F.pure(())
        }

      override def protect[A](fa: F[A]): F[A] = {

        enum AdmittedFrom:
          case Closed(failuresAtAdmission: Int, versionAtAdmission: Long)
          case HalfOpen(versionAtAdmission: Long)

        enum Decision:
          case Run(from: AdmittedFrom)
          case Reject

        enum Result:
          case Success
          case Failure
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
            case ms @ MachineState(InternalState.Broken, _) =>
              ms -> Decision.Reject
          }

        def updateIfCurrent(versionAtAdmission: Long)(evolve: MachineState => MachineState): F[Unit] =
          state.update { ms =>
            if (ms.version === versionAtAdmission) evolve(ms) else ms
          }

        def evolve(from: AdmittedFrom, result: Result)(ms: MachineState): MachineState =
          ms.state match {
            case InternalState.Broken => ms

            case InternalState.HalfOpenRunning =>
              from match {
                case AdmittedFrom.HalfOpen(_) =>
                  result match {
                    case Result.Success  => transitionTo(ms, initClosed)
                    case Result.Failure  => transitionTo(ms, initOpen)
                    case Result.Canceled => transitionTo(ms, InternalState.HalfOpen)
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
                    case Result.Success if failures === failuresAtAdmission =>
                      transitionTo(ms, initClosed)
                    case Result.Failure if failures === failuresAtAdmission =>
                      if (failures + 1 <= maxFailures) transitionTo(ms, InternalState.Closed(failures + 1))
                      else transitionTo(ms, initOpen)
                    case _ => ms
                  }
              }
          }

        admit.flatMap {
          case Decision.Reject =>
            state.get.flatMap(rejectionError)

          case Decision.Run(from) =>
            // Avoid starting work when the breaker has already become terminal.
            ensureNotBroken >> F.guaranteeCase(fa) {
              case Outcome.Succeeded(_) =>
                from match {
                  case AdmittedFrom.HalfOpen(versionAtAdmission) =>
                    updateIfCurrent(versionAtAdmission)(evolve(from, Result.Success))

                  case AdmittedFrom.Closed(failuresAtAdmission, versionAtAdmission) =>
                    updateIfCurrent(versionAtAdmission)(evolve(from, Result.Success))
                }

              case Outcome.Errored(_) =>
                from match {
                  case AdmittedFrom.HalfOpen(versionAtAdmission) =>
                    updateIfCurrent(versionAtAdmission)(evolve(from, Result.Failure))

                  case AdmittedFrom.Closed(failuresAtAdmission, versionAtAdmission) =>
                    updateIfCurrent(versionAtAdmission)(evolve(from, Result.Failure))
                }

              case Outcome.Canceled() =>
                from match {
                  case AdmittedFrom.HalfOpen(versionAtAdmission) =>
                    updateIfCurrent(versionAtAdmission)(evolve(from, Result.Canceled))

                  case AdmittedFrom.Closed(_, _) =>
                    F.unit
                }
            }
        }
      }

      override def attempt[A](fa: F[A]): F[Either[Throwable, A]] = protect(fa).attempt

    }
  }

  final class Builder private[CircuitBreaker] (maxFailures: Int, policy: Policy.type => Policy) {
    require(maxFailures > 0, s"maxFailures should be greater than 0, but got $maxFailures")

    def withMaxFailures(maxFailures: Int): Builder =
      new Builder(maxFailures, policy)

    /** Configure scheduler policy used for Open -> HalfOpen progression.
      *
      * Under strict scheduler semantics, policy should be non-terminating for long-lived breakers.
      * Finite/terminating policies will eventually transition the breaker to Broken.
      */
    def withPolicy(f: Policy.type => Policy): Builder =
      new Builder(maxFailures, f)

    private[CircuitBreaker] def build[F[_]: Async](zoneId: ZoneId): Resource[F, CircuitBreaker[F]] =
      new Impl[F](maxFailures, tickStream.tickScheduled[F](zoneId, policy)).stateMachine
  }

  def apply[F[_]: Async](zoneId: ZoneId, f: Endo[Builder]): Resource[F, CircuitBreaker[F]] =
    f(new Builder(maxFailures = 5, policy = _.empty)).build[F](zoneId)
}
