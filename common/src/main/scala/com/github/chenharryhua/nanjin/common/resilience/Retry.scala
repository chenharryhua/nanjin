package com.github.chenharryhua.nanjin.common.resilience

import cats.Endo
import cats.effect.Temporal
import cats.effect.kernel.Async
import cats.syntax.applicative.given
import cats.syntax.applicativeError.given
import cats.syntax.either.given
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import com.github.chenharryhua.nanjin.common.DurationFormatter
import com.github.chenharryhua.nanjin.common.chrono.{Policy, PolicyTick, TickedValue}
import io.circe.Json
import io.circe.syntax.given

import java.time.{ZoneId, ZonedDateTime}
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.jdk.DurationConverters.{JavaDurationOps, ScalaDurationOps}

/** A `Retry` coordinates repeated execution of effectful computations under a time-based policy.
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
}

object Retry {
  opaque type Attempt = TickedValue[Throwable]

  extension (ra: Attempt)
    // observations
    def failedAt: ZonedDateTime = ra.tick.zoned(_.acquires)
    def cause: Throwable = ra.value
    def attempts: Long = ra.tick.index
    def snooze: FiniteDuration = ra.tick.snooze.toScala
    // transitions
    def followPolicy: Decision = ra.map(Outcome(true))
    def giveUp: Decision = ra.map(Outcome(false))

    /** retryAfter overrides the wake-up time of the next retry attempt while preserving the remaining retry
      * policy.
      */
    def retryAfter(delay: FiniteDuration): Decision =
      ra.withConclude(ra.tick.acquires.plus(delay.toJava)).map(Outcome(true))
  end extension

  final private case class Outcome private (cause: Throwable, retry: Boolean)
  private object Outcome:
    def apply(retry: Boolean)(cause: Throwable): Outcome =
      Outcome(cause, retry)
  end Outcome

  opaque type Decision = TickedValue[Outcome]
  object Decision:
    extension (rd: Decision)
      def cause: Throwable = rd.value.cause
      def toJson: Json = {
        val failed_at = rd.tick.local(_.acquires).asJson
        val attempts = rd.tick.index.asJson
        val zone_id = rd.tick.zoneId.asJson
        if (rd.value.retry)
          Json.obj(
            "retry" -> true.asJson,
            "failed_at" -> failed_at,
            "wakeup_at" -> rd.tick.local(_.conclude).asJson,
            "snooze" -> DurationFormatter.defaultFormatter.format(rd.tick.snooze).asJson,
            "attempts" -> attempts,
            "zone_id" -> zone_id
          )
        else
          Json.obj(
            "retry" -> false.asJson,
            "failed_at" -> failed_at,
            "attempts" -> attempts,
            "zone_id" -> zone_id
          )
      }
    end extension
  end Decision

  final private class Impl[F[_]](seed: PolicyTick[F])(using F: Temporal[F]) {

    def retryLoop[A](fa: F[A], decide: TickedValue[Throwable] => F[TickedValue[Outcome]]): F[A] =
      F.tailRecM[PolicyTick[F], A](seed) { status =>
        F.handleErrorWith(fa.map[Either[PolicyTick[F], A]](Right(_))) { ex =>
          status.advance.flatMap {
            case None       => F.raiseError(ex) // run out of policy
            case Some(next) => // respect user's decision
              decide(TickedValue(next.tick, ex)).attempt.flatMap {
                case Left(decisionEx) =>
                  ex.addSuppressed(decisionEx)
                  F.raiseError(ex)
                case Right(decision) =>
                  if (decision.value.retry)
                    F.sleep(decision.tick.snooze.toScala.max(0.seconds))
                      .as(next.withTick(decision.tick).asLeft[A])
                  else
                    F.raiseError(ex)
              }
          }
        }
      }
  }

  final class Builder[F[_]] private[Retry] (policy: Policy, decide: Attempt => F[Decision]) {

    /** Replaces the decision function used to control retry behavior on failure.
      *
      * The function receives the failed attempt and returns a decision:
      *
      *   - `followPolicy` to continue according to the configured policy
      *   - `retryAfter` to override the next retry delay
      *   - `giveUp` to terminate retrying
      */
    def withDecision(f: Attempt => F[Decision]): Builder[F] =
      new Builder[F](policy, f)

    def withPolicy(f: Policy.type => Policy): Builder[F] =
      new Builder[F](f(Policy), decide)

    private[Retry] def build(zoneId: ZoneId)(using F: Async[F]): F[Retry[F]] =
      PolicyTick.seed[F](zoneId, policy).map { seed =>
        val impl = new Impl[F](seed)
        new Retry[F] {
          override def apply[A](fa: F[A]): F[A] =
            impl.retryLoop(fa, decide)
        }
      }
  }

  def apply[F[_]: Async](zoneId: ZoneId, f: Endo[Builder[F]]): F[Retry[F]] =
    f(new Builder[F](Policy.empty, _.followPolicy.pure[F])).build(zoneId)
}
