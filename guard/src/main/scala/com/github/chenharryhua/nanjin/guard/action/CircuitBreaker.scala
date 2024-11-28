package com.github.chenharryhua.nanjin.guard.action

import cats.effect.kernel.{Async, Resource}
import cats.implicits.{catsSyntaxApplicativeError, catsSyntaxMonadErrorRethrow, toFlatMapOps}
import com.github.chenharryhua.nanjin.common.chrono.{tickStream, Policy, Tick}
import fs2.Stream

import java.time.ZoneId

trait CircuitBreaker[F[_]] {
  def apply[A](fa: => F[A]): F[A]
}

object CircuitBreaker {
  sealed private trait State extends Product with Serializable

  private object State {
    final case class Closed(failures: Int) extends State
    final case object HalfOpen extends State
    final case object Open extends State
  }

  final case object RejectedException extends Exception("CircuitBreaker Rejected Exception")

  final private class Impl[F[_]](maxFailures: Int, ticks: Stream[F, Tick])(implicit F: Async[F]) {

    private val initClosed: State = State.Closed(1)

    val stateMachine: Resource[F, CircuitBreaker[F]] = for {
      state <- Resource.eval(F.ref[State](initClosed))
      _ <- F.background(
        ticks
          .evalMap(_ =>
            state.update {
              case State.Open      => State.HalfOpen
              case State.Closed(_) => initClosed
              case State.HalfOpen  => State.HalfOpen
            })
          .compile
          .drain)
    } yield new CircuitBreaker[F] {
      override def apply[A](fa: => F[A]): F[A] = {
        def updateState(result: Either[Throwable, A]): F[Unit] = result match {
          case Left(_) =>
            state.update {
              case State.Closed(failures) =>
                if (failures < maxFailures) State.Closed(failures + 1) else State.Open
              case State.HalfOpen => State.Open
              case State.Open     => State.Open
            }
          case Right(_) =>
            state.update {
              case cls @ State.Closed(_) => cls
              case State.HalfOpen        => initClosed
              case State.Open            => State.Open
            }
        }

        F.uncancelable(poll =>
          state.get.flatMap {
            case State.Open => F.raiseError(RejectedException)
            case _          => poll(F.defer(fa)).attempt.flatTap(updateState).rethrow
          })
      }
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
      new Impl[F](maxFailures, tickStream.fromOne[F](policy, zoneId)).stateMachine
  }
}
