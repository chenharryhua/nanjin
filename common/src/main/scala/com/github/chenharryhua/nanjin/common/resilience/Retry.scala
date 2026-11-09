package com.github.chenharryhua.nanjin.common.resilience

import cats.Endo
import cats.data.Kleisli
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
import org.apache.commons.lang3.exception.ExceptionUtils

import java.time.{Duration, Instant, ZoneId, ZonedDateTime}
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.jdk.DurationConverters.{JavaDurationOps, ScalaDurationOps}

/** A `Retry` coordinates repeated execution of effectful computations under a time-based policy.
  *
  * A retry is governed by two orthogonal concerns:
  *
  *   1. A `Policy` that defines the temporal structure of retry attempts (limits, delays, backoff)
  *   2. A *decision function* that is invoked after a failure when the policy supplies another retry tick. It
  *      determines whether execution should continue and may reshape that retry time-frame
  *
  * ===Attempt context===
  *
  * The decision function receives an `Attempt` carrying:
  *   - `cause` — the current exception
  *   - `previousCause` — the exception from the prior attempt (`None` on first failure)
  *   - `ordinal` — how many failures have occurred (1-based)
  *   - `elapsed` — real wall-clock time since the first failure as `FiniteDuration` (includes both sleep and
  *     execution time, not just accumulated policy delays)
  *   - `snooze` — the delay the policy proposes before the next attempt. When accepted, the full delay is
  *     slept after the decision effect completes
  *   - `failedAt` — the zoned timestamp of the failure
  *
  * ===Decision transitions===
  *
  * The decision function returns one of:
  *   - `followPolicy` — accept the policy's proposed delay and continue
  *   - `retryAfter(delay)` — override the next delay while keeping the current policy
  *   - `giveUp` — stop retrying and propagate the failure
  *
  * A decision function that fails instead of returning one of these stops the retry and raises
  * `DecisionFailure`, which carries both the decision failure and the operation failure.
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
    * The decision function is not invoked when the policy supplies no retry tick, including the terminal
    * failure after policy exhaustion. Only the last failure is propagated if execution ultimately fails. A
    * failure in the decision function itself instead surfaces as `Retry.DecisionFailure`, which carries both
    * that failure and the operation's.
    */
  def apply[A](fa: F[A]): F[A]
}

object Retry {
  def noop[F[_]]: Retry[F] = new Retry[F] {
    override def apply[A](fa: F[A]): F[A] = fa
  }

  /** Raised when the decision function fails, in place of the operation's own failure.
    *
    * Both failures are preserved and nothing is mutated, this exception included. The decision failure is its
    * `cause`, because that is what stopped the retry loop, and the operation failure is its
    * `operationFailure` and is named in its message.
    *
    * The message is what carries the operation failure into logs, because the `cause` slot holds the decision
    * failure and nothing else refers to it. Renderers differ in how much they follow:
    * `ExceptionUtils.getRootCauseStackTraceList`, behind guard's `StackTrace`, walks only the cause chain,
    * while `printStackTrace` would also print suppressed exceptions if there were any. Since neither failure
    * is attached to the other, the operation failure's own '''stack trace''' is printed nowhere; read
    * `operationFailure` when the trace matters and not just the message.
    *
    * Nothing is attached deliberately. `Throwable.addSuppressed` has no counterpart that removes an entry,
    * and a throwable can outlive the call that raised it: a Scala `case object` such as
    * `CircuitBreaker.RejectedException` is a single instance for the life of the JVM, so attaching to the
    * operation failure on each decision failure would grow that shared instance without bound. Composing into
    * a new exception avoids touching a throwable `Retry` did not create.
    *
    * A decision function that raises the attempt's own cause still produces a `DecisionFailure`. To stop
    * retrying and propagate the operation failure unchanged, return `giveUp` instead of raising.
    *
    * @param operationFailure
    *   the failure raised by the retried operation; named in this exception's message, and the only place its
    *   stack trace remains reachable
    * @param decisionFailure
    *   the failure raised by the decision function; also this exception's `cause`
    */
  final class DecisionFailure private[Retry] (val operationFailure: Throwable, val decisionFailure: Throwable)
      extends Exception(
        s"retry decision failed; operation failed with ${ExceptionUtils.getMessage(operationFailure)}",
        decisionFailure)

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

      /** The zoned timestamp of this failure.
        *
        * Sampled when the policy advances after the operation threw, not at the exact instant of the throw.
        * It is therefore shortly after the true failure by the policy-advance latency.
        */
      def failedAt: ZonedDateTime = ra.tick.zoned(_.acquires)
      def cause: Throwable = ra.cause
      def ordinal: Long = ra.tick.index
      def snooze: FiniteDuration = ra.tick.snooze.toScala
      def previousCause: Option[Throwable] = ra.previousCause

      /** Real wall-clock time since the first failure.
        *
        * Both endpoints are `failedAt`-style samples taken when the policy advances, so this spans sleep, the
        * operation's own runtime, and coordination and decision-function time. It is not a sum of policy
        * delays and is not measured from the literal throw instant.
        */
      def elapsed: FiniteDuration = Duration.between(ra.firstFailureAt, ra.tick.acquires).toScala

      // transitions
      def followPolicy: Decision = Decision(ra.tick)

      /** Override the next retry delay.
        *
        * Negative values are normalized to zero. After the decision effect completes, the retry loop sleeps
        * the full normalized delay. The encoded `wakeup_at` remains the tick's proposed pre-decision
        * timestamp, so decision latency and scheduler delay can make the actual retry later; encoded `snooze`
        * is the normalized delay itself.
        */
      def retryAfter(delay: FiniteDuration): Decision =
        Decision(ra.tick.withConclude(ra.tick.acquires.plus(delay.max(0.seconds).toJava)))
      def giveUp: Decision = Decision.stop(ra.tick)
    end extension
  end Attempt

  final private case class DecisionData(tick: Tick, accepted: Boolean)

  /** A retry transition bound to the `Attempt` that created it.
    *
    * A decision carries that attempt's retry-plan tick and must be returned only from the corresponding
    * decision-function invocation. Retaining it or reusing it for another attempt is unsupported and can
    * replace current retry state with stale timing or sequence metadata.
    */
  opaque type Decision = DecisionData
  object Decision:
    private[Retry] def apply(tick: Tick): Decision = DecisionData(tick, true)
    private[Retry] def stop(tick: Tick): Decision = DecisionData(tick, false)

    extension (rd: Decision) def accepted: Boolean = rd.accepted

    /** Encodes the decision and its proposed timing metadata.
      *
      * For accepted decisions, `wakeup_at` is calculated from the policy tick before the decision effect
      * runs; it is not a guarantee of the actual retry time. The retry begins only after the decision
      * completes and the encoded `snooze` has been slept, subject to scheduler delay.
      */
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
    F: Async[F]) {

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
              F.defer(decide.run(attempt)).attempt.flatMap {
                case Left(decision_error) =>
                  F.raiseError(new DecisionFailure(ex, decision_error))
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

    /** Replaces the decision function used to control retry behavior after a failure when the policy supplies
      * another retry tick. The function is not invoked for the terminal failure after policy exhaustion.
      *
      * The function receives the failed attempt (including cause, ordinal, timing, previousCause, elapsed,
      * and snooze) and returns a decision:
      *
      *   - `followPolicy` to continue according to the configured policy
      *   - `retryAfter` to override the next retry delay
      *   - `giveUp` to terminate retrying
      *
      * Return a decision created from the `Attempt` passed to the current invocation. Decisions are
      * attempt-bound; retaining or reusing one across attempts is unsupported.
      *
      * Failures returned in `F`, or thrown while constructing it, stop retrying and surface as a
      * `DecisionFailure` carrying both errors: the decision failure as its `cause`, the operation failure as
      * `operationFailure`. Neither original throwable is modified. To stop retrying and propagate the
      * operation failure unchanged, return `giveUp` rather than raising.
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
    Async[F].defer(f(new Builder[F](Policy.empty, Kleisli(_.followPolicy.pure[F]))).build(zoneId))
  }
}
