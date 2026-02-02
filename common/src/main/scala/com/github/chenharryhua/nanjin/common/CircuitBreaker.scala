package com.github.chenharryhua.nanjin.common

import cats.Endo
import cats.effect.implicits.genSpawnOps
import cats.effect.kernel.{Async, Outcome, Resource}
import cats.implicits.{catsSyntaxApplicativeError, toFlatMapOps}
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
  sealed trait State extends Product
  implicit val encoderState: Encoder[State] = {
    case State.Closed(failures) =>
      Json.obj("state" -> Json.fromString("Closed"), "failures" -> Json.fromInt(failures))
    case State.HalfOpen | State.HalfOpenRunning =>
      Json.obj("state" -> Json.fromString("Half-Open"))
    case State.Open(rejects) =>
      Json.obj("state" -> Json.fromString("Open"), "rejects" -> Json.fromInt(rejects))
  }

  private object State {
    final case class Closed(failures: Int) extends State
    case object HalfOpen extends State
    private[CircuitBreaker] case object HalfOpenRunning extends State
    final case class Open(rejects: Int) extends State
  }

  case object RejectedException extends Exception("CircuitBreaker Rejected Exception") with NoStackTrace {
    override def fillInStackTrace(): Throwable = this
  }

  final private class Impl[F[_]](maxFailures: Int, ticks: Stream[F, Tick])(implicit F: Async[F]) {

    private val initClosed: State = State.Closed(0)
    private val initOpen: State = State.Open(0)

    val stateMachine: Resource[F, CircuitBreaker[F]] = for {
      state <- Resource.eval(F.ref[State](initClosed))
      _ <- ticks.evalMap { _ =>
        state.update {
          case State.Open(_) => State.HalfOpen
          case other         => other
        }
      }.compile.drain.background
    } yield new CircuitBreaker[F] {

      override val getState: F[State] = state.get

      override def protect[A](fa: F[A]): F[A] = {

        sealed trait Decision
        case object Run extends Decision
        case object Reject extends Decision

        val admit: F[Decision] =
          state.modify {
            case State.Open(rejects)      => State.Open(rejects + 1) -> Reject
            case State.HalfOpen           => State.HalfOpenRunning -> Run
            case State.HalfOpenRunning    => State.HalfOpenRunning -> Reject
            case closed @ State.Closed(_) => closed -> Run
          }

        admit.flatMap {
          case Reject => F.raiseError(RejectedException)

          case Run =>
            F.guaranteeCase(fa) {
              case Outcome.Succeeded(_) =>
                state.update {
                  case State.HalfOpenRunning => initClosed
                  case State.HalfOpen        => initClosed
                  case State.Closed(_)       => initClosed
                  case open @ State.Open(_)  => open
                }

              case Outcome.Errored(_) =>
                state.update {
                  case State.HalfOpenRunning  => initOpen
                  case State.HalfOpen         => initOpen
                  case State.Closed(failures) =>
                    if (failures + 1 < maxFailures) State.Closed(failures + 1) else initOpen
                  case open @ State.Open(_) => open
                }

              case Outcome.Canceled() =>
                state.update {
                  case State.HalfOpenRunning => initOpen
                  case other                 => other
                }
            }
        }
      }

      override def attempt[A](fa: F[A]): F[Either[Throwable, A]] = protect(fa).attempt

    }
  }

  final class Builder private[CircuitBreaker] (maxFailures: Int, policy: Policy) {
    def withMaxFailures(maxFailures: Int): Builder =
      new Builder(maxFailures, policy)

    def withPolicy(policy: Policy): Builder =
      new Builder(maxFailures, policy)

    def withPolicy(f: Policy.type => Policy): Builder =
      new Builder(maxFailures, f(Policy))

    private[CircuitBreaker] def build[F[_]: Async](zoneId: ZoneId): Resource[F, CircuitBreaker[F]] =
      new Impl[F](maxFailures, tickStream.tickScheduled[F](zoneId, policy)).stateMachine
  }

  def apply[F[_]: Async](zoneId: ZoneId, f: Endo[Builder]): Resource[F, CircuitBreaker[F]] =
    f(new Builder(maxFailures = 5, policy = Policy.giveUp)).build[F](zoneId)
}
