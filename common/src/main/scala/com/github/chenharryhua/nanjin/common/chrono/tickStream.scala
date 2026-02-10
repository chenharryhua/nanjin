package com.github.chenharryhua.nanjin.common.chrono

import cats.effect.kernel.{Async, Temporal}
import cats.implicits.{toFlatMapOps, toFunctorOps, toTraverseOps}
import fs2.{Pull, Stream}

import java.time.{Duration as JavaDuration, Instant, ZoneId}
import java.util.UUID
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.jdk.DurationConverters.{JavaDurationOps, ScalaDurationOps}
import scala.util.Random

object tickStream {

  /** Convert a TickStatus into a stream of Tick. For each status:
    *   - Compute the next tick based on the current time
    *   - Sleep for the tick’s snooze duration
    *   - Emit the tick
    */
  def fromTickStatus[F[_]](zeroth: TickStatus)(implicit F: Temporal[F]): Stream[F, Tick] =
    Stream.unfoldEval[F, TickStatus, Tick](zeroth) { status =>
      F.realTimeInstant.flatMap { now =>
        status.next(now).traverse(nt => F.sleep(nt.tick.snooze.toScala).as((nt.tick, nt)))
      }
    }

  /** Stream ticks in a **“sleep first, then emit”** pattern.
    *
    * Each tick represents a structured time-frame governed by a `Policy`.
    *
    * Behavior:
    *   - The **first tick** (index = 1) is **not emitted immediately**; the stream waits for the tick's
    *     `snooze` first.
    *   - After sleeping for `tick.snooze`, the tick is emitted downstream.
    *   - If downstream processing takes longer than the tick’s duration, the next tick still waits for its
    *     **full snooze**.
    *
    * ===Key semantics===
    *
    *   - `tick.conclude` represents the **moment the downstream actually receives the tick**.
    *   - Strict, time-driven schedule: ticks always respect the planned snooze/duration.
    *
    * ===Notes===
    *
    *   - The stream terminates automatically when the policy has no further decisions (e.g., `GiveUp` or
    *     `Limited` exhausted).
    *
    * @param zoneId
    *   The time zone used for generating ticks
    * @param policy
    *   The scheduling/retry policy controlling tick evolution
    * @tparam F
    *   Effect type supporting asynchronous operations (e.g., `IO`)
    * @return
    *   A `Stream[F, Tick]` that emits sequential ticks according to the policy and sleeps first
    */
  def tickScheduled[F[_]: Async](zoneId: ZoneId, policy: Policy): Stream[F, Tick] =
    Stream.eval[F, TickStatus](TickStatus.zeroth[F](zoneId, policy)).flatMap(fromTickStatus[F])

  /** Stream ticks starting from the zeroth tick. The first tick (index = 0) is emitted immediately, then
    * continues according to the policy.
    */
  def tickImmediate[F[_]: Async](zoneId: ZoneId, policy: Policy): Stream[F, Tick] =
    Stream
      .eval[F, TickStatus](TickStatus.zeroth[F](zoneId, policy))
      .flatMap(zero => fromTickStatus[F](zero).cons1(zero.tick))

  /** Stream ticks in an **“emit first, then sleep”** pattern.
    *
    * Each tick represents a structured time-frame governed by a `Policy`.
    *
    * Behavior:
    *   - The **first tick** (index = 1) is emitted immediately.
    *   - Each subsequent tick is computed according to the policy sequence.
    *   - After emission, the stream sleeps until the tick’s `conclude` time.
    *   - If downstream processing takes longer than the tick's duration, the stream **skips the sleep** and
    *     produces the next tick immediately.
    *
    * ===Key semantics===
    *
    *   - `tick.acquires` represents the **moment the downstream actually receives the tick**.
    *   - Tick emission may **drift relative to the nominal schedule** if processing is slow.
    *
    * ===Notes===
    *
    *   - The stream terminates automatically when the policy has no further decisions (e.g., `GiveUp` or
    *     `Limited` exhausted).
    *
    * @param zoneId
    *   The time zone used for generating ticks
    * @param policy
    *   The scheduling/retry policy controlling tick evolution
    * @tparam F
    *   Effect type supporting asynchronous operations (e.g., `IO`)
    * @return
    *   A `Stream[F, Tick]` that emits sequential ticks respecting the policy timing
    */
  def tickFuture[F[_]](zoneId: ZoneId, policy: Policy)(implicit F: Async[F]): Stream[F, Tick] =
    Stream.eval[F, TickStatus](TickStatus.zeroth[F](zoneId, policy)).flatMap { status =>
      def go(status: TickStatus): Pull[F, Tick, Unit] =
        Pull.eval(F.realTimeInstant.map(status.next)).flatMap {
          case None     => Pull.done
          case Some(ts) =>
            Pull.output1(ts.tick) >> // do evalMap etc at this moment
              Pull.eval(F.realTimeInstant).flatMap { now =>
                val conclude: Instant = ts.tick.conclude
                if (now.isBefore(conclude)) {
                  val gap: FiniteDuration = JavaDuration.between(now, conclude).toScala
                  Pull.sleep(gap) >> go(ts)
                } else go(ts) // cross border
              }
        }
      go(status).stream
    }
}

/** LazyList generators for testing or simulation.
  */
object tickLazyList {

  /** Generate an infinite LazyList of ticks starting from an initial TickStatus. Each next tick is delayed by
    * a random 1–5 milliseconds for testing purposes.
    */
  def fromTickStatus(init: TickStatus): LazyList[Tick] =
    LazyList.unfold(init)(ts =>
      ts.next(ts.tick.conclude.plus(Random.between(1, 5).milliseconds.toJava)).map(s => (s.tick, s)))

  /** Generate ticks starting from a zeroth tick with the given ZoneId and policy.
    */
  def from(zoneId: ZoneId, policy: Policy): LazyList[Tick] =
    fromTickStatus(TickStatus(Tick.zeroth(UUID.randomUUID(), zoneId, Instant.now())).renewPolicy(policy))

  /** Generate ticks with the default system ZoneId.
    */
  def from(policy: Policy): LazyList[Tick] =
    from(ZoneId.systemDefault(), policy)
}
