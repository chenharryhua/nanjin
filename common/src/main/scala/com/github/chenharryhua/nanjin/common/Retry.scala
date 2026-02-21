package com.github.chenharryhua.nanjin.common

import cats.Endo
import cats.effect.Temporal
import cats.effect.kernel.{Async, Resource}
import cats.effect.std.Hotswap
import cats.syntax.applicative.catsSyntaxApplicativeId
import cats.syntax.either.catsSyntaxEitherId
import cats.syntax.flatMap.{catsSyntaxFlatMapOps, toFlatMapOps}
import cats.syntax.functor.toFunctorOps
import com.github.chenharryhua.nanjin.common.chrono.{Policy, PolicyTick, TickedValue}

import java.time.ZoneId
import scala.concurrent.duration.DurationInt
import scala.jdk.DurationConverters.JavaDurationOps

/** A `Retry` coordinates repeated execution of effectful computations and resource acquisition under a
  * time-based policy.
  *
  * A retry is governed by two orthogonal concerns:
  *
  *   1. A [[com.github.chenharryhua.nanjin.common.chrono.Policy]] that defines the temporal structure of
  *      retry attempts (limits, delays, backoff)
  *   2. A *decision function* that is invoked on failure and determines whether execution should continue,
  *      optionally reshaping the next retry time-frame
  *
  * Retry execution is driven by a sequence of evolving [[com.github.chenharryhua.nanjin.common.chrono.Tick]]
  * instances, allowing retry behavior to be time-aware, observable, and adaptive.
  *
  * `Retry` is a coordination mechanism only: it does not impose semantics on the effect itself, but
  * re-invokes it according to the configured policy and decision logic.
  *
  * A `Retry[F]` instance is immutable and may be safely reused.
  */
trait Retry[F[_]] {

  /** Executes the given effect, retrying failures according to the configured policy and decision function.
    *
    * Only the last failure is propagated if execution ultimately fails.
    */
  def apply[A](fa: F[A]): F[A]

  /** Executes the acquisition of the given resource, retrying acquisition failures according to the
    * configured policy and decision function.
    *
    * Only resource acquisition is retried; failures during resource usage are not intercepted.
    *
    * Resource finalizers may be invoked multiple times if retries occur.
    *
    * Only the last acquisition failure is propagated if all retries fail.
    */
  def apply[A](rfa: Resource[F, A]): Resource[F, A]
}

object Retry {

  final private class Impl[F[_]](initTS: PolicyTick[F])(implicit F: Temporal[F]) {

    def retryLoop[A](fa: F[A], decide: TickedValue[Throwable] => F[TickedValue[Boolean]]): F[A] =
      F.tailRecM[PolicyTick[F], A](initTS) { status =>
        F.handleErrorWith(fa.map[Either[PolicyTick[F], A]](Right(_))) { ex =>
          F.realTimeInstant.flatMap(status.next).flatMap {
            case None     => F.raiseError(ex) // run out of policy
            case Some(ts) => // respect user's decision
              decide(TickedValue(ts.tick, ex)).flatMap { tv =>
                if (tv.value)
                  F.sleep(tv.tick.snooze.toScala.max(0.seconds)).as(ts.withTick(tv.tick).asLeft[A])
                else
                  F.raiseError(ex)
              }
          }
        }
      }

    // Drop any previously acquired resource before retrying acquisition.
    // A failed acquisition poisons the previous resource; retries must start fresh.
    def resource[A](
      rfa: Resource[F, A],
      decide: TickedValue[Throwable] => F[TickedValue[Boolean]]): Resource[F, A] =
      Hotswap.create[F, A].evalMap(hotswap => retryLoop(hotswap.clear >> hotswap.swap(rfa), decide))
  }

  final class Builder[F[_]] private[Retry] (
    policy: Policy,
    decide: TickedValue[Throwable] => F[TickedValue[Boolean]]) {

    /** Replaces the decision function used to control retry behavior on failure.
      *
      * The function receives the failure annotated with the current 'Tick'.
      *
      * It determines:
      *   - whether execution should continue (`true`) or terminate (`false`)
      *   - the timing of the next retry attempt, via modifications to the provided `Tick`
      */
    def withDecision(f: TickedValue[Throwable] => F[TickedValue[Boolean]]): Builder[F] =
      new Builder[F](policy, f)

    def withPolicy(f: Policy.type => Policy): Builder[F] =
      new Builder[F](f(Policy), decide)

    private[Retry] def build(zoneId: ZoneId)(implicit F: Async[F]): F[Retry[F]] =
      PolicyTick.zeroth[F](zoneId, policy).map { ts =>
        val impl = new Impl[F](ts)
        new Retry[F] {
          override def apply[A](fa: F[A]): F[A] =
            impl.retryLoop(fa, decide)

          override def apply[A](rfa: Resource[F, A]): Resource[F, A] =
            impl.resource(rfa, decide)
        }
      }
  }

  def apply[F[_]: Async](zoneId: ZoneId, f: Endo[Builder[F]]): F[Retry[F]] =
    f(new Builder[F](Policy.empty, _.map(_ => true).pure[F])).build(zoneId)

}
