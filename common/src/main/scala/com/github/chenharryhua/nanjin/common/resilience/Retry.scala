package com.github.chenharryhua.nanjin.common.resilience

import cats.Endo
import cats.data.Kleisli
import cats.effect.Temporal
import cats.effect.kernel.Async
import cats.syntax.applicative.given
import cats.syntax.applicativeError.given
import cats.syntax.either.given
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import com.github.chenharryhua.nanjin.common.DurationFormatter
import com.github.chenharryhua.nanjin.common.chrono.{Policy, PolicyTick, Tick}
import io.circe.syntax.given
import io.circe.{Encoder, Json}

import java.time.{Duration, Instant, ZoneId, ZonedDateTime}
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.jdk.DurationConverters.{JavaDurationOps, ScalaDurationOps}

/** A `Retry` coordinates repeated execution of effectful computations under a time-based policy.
  *
  * A retry is governed by two orthogonal concerns:
  *
  *   1. A `Policy` that defines the temporal structure of retry attempts (limits, delays, backoff)
  *   2. A *decision function* that is invoked on failure and determines whether execution should continue,
  *      optionally reshaping the next retry time-frame
  *
  * ===Attempt context===
  *
  * The decision function receives an `Attempt` carrying:
  *   - `cause` — the current exception
  *   - `previousCause` — the exception from the prior attempt (`None` on first failure)
  *   - `ordinal` — how many failures have occurred (1-based)
  *   - `elapsed` — real wall-clock time since the first failure as `FiniteDuration` (includes both sleep and
  *     execution time, not just accumulated policy delays)
  *   - `snooze` — the delay the policy proposes before the next attempt
  *   - `failedAt` — the zoned timestamp of the failure
  *
  * ===Decision transitions===
  *
  * The decision function returns one of:
  *   - `followPolicy` — accept the policy's proposed delay and continue
  *   - `retryAfter(delay)` — override the next delay while keeping the current policy
  *   - `giveUp` — stop retrying and propagate the failure
  *
  * ===Design===
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
  final private case class AttemptData(
    tick: Tick,
    cause: Throwable,
    previousCause: Option[Throwable],
    firstFailureAt: Instant)

  opaque type Attempt = AttemptData
  object Attempt:
    private[Retry] def apply(
      tick: Tick,
      cause: Throwable,
      previousCause: Option[Throwable],
      firstFailureAt: Instant): Attempt =
      AttemptData(tick, cause, previousCause, firstFailureAt)

    extension (ra: Attempt)
      // observations
      def failedAt: ZonedDateTime = ra.tick.zoned(_.acquires)
      def cause: Throwable = ra.cause
      def ordinal: Long = ra.tick.index
      def snooze: FiniteDuration = ra.tick.snooze.toScala
      def previousCause: Option[Throwable] = ra.previousCause
      def elapsed: FiniteDuration = Duration.between(ra.firstFailureAt, ra.tick.acquires).toScala

      // transitions
      def followPolicy: Decision = Decision(ra.tick)
      def retryAfter(delay: FiniteDuration): Decision =
        Decision(ra.tick.withConclude(ra.tick.acquires.plus(delay.toJava)))
      def giveUp: Decision = Decision.stop(ra.tick)
    end extension
  end Attempt

  final private case class DecisionData(tick: Tick, accepted: Boolean)
  opaque type Decision = DecisionData
  object Decision:
    private[Retry] def apply(tick: Tick): Decision = DecisionData(tick, true)
    private[Retry] def stop(tick: Tick): Decision = DecisionData(tick, false)

    extension (rd: Decision) def accepted: Boolean = rd.accepted

    given Encoder[Decision] = Encoder.instance { rd =>
      val tick = rd.tick
      val failed_at = tick.local(_.acquires).asJson
      val ordinal = tick.index.asJson
      val zone_id = tick.zoneId.asJson
      if (rd.accepted)
        Json.obj(
          "retry" -> true.asJson,
          "failed_at" -> failed_at,
          "wakeup_at" -> tick.local(_.conclude).asJson,
          "snooze" -> DurationFormatter.defaultFormatter.format(tick.snooze).asJson,
          "ordinal" -> ordinal,
          "zone_id" -> zone_id
        )
      else
        Json.obj(
          "retry" -> false.asJson,
          "failed_at" -> failed_at,
          "ordinal" -> ordinal,
          "zone_id" -> zone_id
        )
    }
  end Decision

  final private class Impl[F[_]](seed: PolicyTick[F], decide: Kleisli[F, Attempt, Decision])(using
    F: Temporal[F]) {

    private case class LoopState(
      policyTick: PolicyTick[F],
      previousCause: Option[Throwable],
      firstFailureAt: Option[Instant])

    def retryLoop[A](fa: F[A]): F[A] =
      F.tailRecM[LoopState, A](LoopState(seed, None, None)) { state =>
        F.handleErrorWith(fa.map[Either[LoopState, A]](Right(_))) { ex =>
          state.policyTick.advance.flatMap {
            case None       => F.raiseError(ex) // run out of policy
            case Some(next) => // respect user's decision
              val firstFailure = state.firstFailureAt.getOrElse(next.tick.acquires)
              val attempt = Attempt(next.tick, ex, state.previousCause, firstFailure)
              decide.run(attempt).attempt.flatMap {
                case Left(decisionEx) =>
                  ex.addSuppressed(decisionEx)
                  F.raiseError(ex)
                case Right(decision) =>
                  if (decision.accepted)
                    val nextState = next.withTick(decision.tick)
                    F.sleep(decision.tick.snooze.toScala.max(0.seconds))
                      .as(LoopState(nextState, Some(ex), Some(firstFailure)).asLeft[A])
                  else F.raiseError(ex)
              }
          }
        }
      }
  }

  final class Builder[F[_]] private[Retry] (policy: Policy, decide: Kleisli[F, Attempt, Decision]) {

    /** Replaces the decision function used to control retry behavior on failure.
      *
      * The function receives the failed attempt (including cause, ordinal, timing, previousCause, elapsed,
      * and snooze) and returns a decision:
      *
      *   - `followPolicy` to continue according to the configured policy
      *   - `retryAfter` to override the next retry delay
      *   - `giveUp` to terminate retrying
      */
    def withDecision(f: Attempt => F[Decision]): Builder[F] =
      new Builder[F](policy, Kleisli(f))

    def withPolicy(f: Policy.type => Policy): Builder[F] =
      new Builder[F](f(Policy), decide)

    private[Retry] def build(zoneId: ZoneId)(using F: Async[F]): F[Retry[F]] =
      PolicyTick.seed[F](zoneId, policy).map { seed =>
        val impl = new Impl[F](seed, decide)
        new Retry[F] {
          override def apply[A](fa: F[A]): F[A] = impl.retryLoop(fa)
        }
      }
  }

  def apply[F[_]: Async](zoneId: ZoneId, f: Endo[Builder[F]]): F[Retry[F]] = {
    import Attempt.followPolicy
    f(new Builder[F](Policy.empty, Kleisli(_.followPolicy.pure[F]))).build(zoneId)
  }
}
