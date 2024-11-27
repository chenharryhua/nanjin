package com.github.chenharryhua.nanjin.guard.action

import cats.effect.kernel.{Async, Resource}
import cats.effect.std.Queue
import cats.implicits.{catsSyntaxApplicativeError, catsSyntaxApplyOps, toFlatMapOps, toFunctorOps}
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

  final private class Impl[F[_]](maxFailures: Int, maxConcurrent: Int, ticks: Stream[F, Tick])(implicit
    F: Async[F]) {

    private val initClosed: State = State.Closed(1)

    val stateMachine: Resource[F, CircuitBreaker[F]] = for {
      state <- Resource.eval(F.ref[State](initClosed))
      queue <- Resource.eval(Queue.unbounded[F, F[Unit]])
      _ <- F.background(
        Stream
          .fromQueueUnterminated[F, F[Unit]](queue)
          .parEvalMapUnordered(maxConcurrent)(identity)
          .concurrently(ticks.evalMap(_ =>
            state.update {
              case State.Open => State.HalfOpen
              case others     => others
            }))
          .compile
          .drain)
    } yield new CircuitBreaker[F] {
      override def apply[A](fa: => F[A]): F[A] =
        F.deferred[Either[Throwable, A]].flatMap { promise =>
          val updateEval: F[Unit] = F.defer(fa).attempt.flatTap(promise.complete).flatMap {
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

          val doWork: F[Unit] =
            state.get.flatMap {
              case State.Closed(_) => updateEval
              case State.HalfOpen  => updateEval
              case State.Open      => promise.complete(Left(RejectedException)).void
            }

          queue.offer(doWork) *> F.rethrow(promise.get)
        }
    }
  }

  final class Builder private[guard] (maxFailures: Int, maxConcurrent: Int, policy: Policy) {
    def withMaxFailures(maxFailures: Int): Builder =
      new Builder(maxFailures, maxConcurrent, policy)

    def withMaxConcurrent(maxConcurrent: Int): Builder =
      new Builder(maxFailures, maxConcurrent, policy)

    def withPolicy(policy: Policy): Builder =
      new Builder(maxFailures, maxConcurrent, policy)

    def withPolicy(f: Policy.type => Policy): Builder =
      new Builder(maxFailures, maxConcurrent, f(Policy))

    private[guard] def build[F[_]: Async](zoneId: ZoneId): Resource[F, CircuitBreaker[F]] =
      new Impl[F](maxFailures, maxConcurrent, tickStream.fromOne[F](policy, zoneId)).stateMachine
  }
}
