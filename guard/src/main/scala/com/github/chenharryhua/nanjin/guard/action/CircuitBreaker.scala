package com.github.chenharryhua.nanjin.guard.action

import cats.effect.kernel.{Async, Resource}
import cats.implicits.{catsSyntaxApplicativeError, catsSyntaxApplyOps, toFlatMapOps}
import com.github.chenharryhua.nanjin.common.chrono.{tickStream, Policy, Tick}
import fs2.Stream
import io.circe.{Encoder, Json}

import java.time.ZoneId

trait CircuitBreaker[F[_]] {
  def attempt[A](fa: F[A]): F[Either[Throwable, A]]
  def protect[A](fa: F[A]): F[A]
  def getState: F[CircuitBreaker.State]
}

object CircuitBreaker {
  sealed trait State extends Product
  implicit val encoderState: Encoder[State] = {
    case State.Closed(failures) => Json.fromString(s"[Closed: $failures failures]")
    case State.HalfOpen         => Json.fromString("[Half-Open]")
    case State.Open(rejects)    => Json.fromString(s"[Open: $rejects rejects]")
  }
  private object State {
    final case class Closed(failures: Int) extends State
    final case object HalfOpen extends State
    final case class Open(rejects: Int) extends State
  }

  final case object RejectedException extends Exception("CircuitBreaker Rejected Exception")

  final private class Impl[F[_]](maxFailures: Int, ticks: Stream[F, Tick])(implicit F: Async[F]) {

    private val initClosed: State = State.Closed(0)
    private val initOpen: State = State.Open(0)

    val stateMachine: Resource[F, CircuitBreaker[F]] = for {
      state <- Resource.eval(F.ref[State](initClosed))
      _ <- F.background(
        ticks
          .evalMap(_ =>
            state.update {
              case State.Open(_)   => State.HalfOpen
              case State.Closed(_) => initClosed
              case State.HalfOpen  => State.HalfOpen
            })
          .compile
          .drain)
    } yield new CircuitBreaker[F] {

      override val getState: F[State] = state.get

      override def attempt[A](fa: F[A]): F[Either[Throwable, A]] = {
        def updateState(result: Either[Throwable, A]): F[Unit] =
          state.update {
            case keep @ State.Closed(failures) =>
              result match {
                case Left(_)  => if (failures < maxFailures) State.Closed(failures + 1) else initOpen
                case Right(_) => keep
              }
            case State.HalfOpen =>
              result match {
                case Left(_)  => initOpen
                case Right(_) => initClosed
              }
            case keep @ State.Open(rejects) =>
              result match {
                case Left(_)  => State.Open(rejects + 1)
                case Right(_) => keep
              }
          }

        F.uncancelable(poll =>
          state.get.flatMap {
            case State.Open(_) =>
              val rejected: Left[RejectedException.type, A] = Left(RejectedException)
              updateState(rejected) *> F.pure(rejected)
            case _ => poll(fa).attempt.flatTap(updateState)
          })
      }

      override def protect[A](fa: F[A]): F[A] = F.rethrow(attempt(fa))
    }
  }

  final class Builder private[guard] (maxFailures: Int, policy: Policy) {
    def withMaxFailures(maxFailures: Int): Builder =
      new Builder(maxFailures, policy)

    def withPolicy(policy: Policy): Builder =
      new Builder(maxFailures, policy)

    def withPolicy(f: Policy.type => Policy): Builder =
      new Builder(maxFailures, f(Policy))

    private[guard] def build[F[_]: Async](zoneId: ZoneId): Resource[F, CircuitBreaker[F]] =
      new Impl[F](maxFailures - 1, tickStream.tickScheduled[F](zoneId, policy)).stateMachine
  }
}
