package com.github.chenharryhua.nanjin.common.chrono

import cats.effect.kernel.{Async, Sync, Temporal}
import cats.effect.std.Random
import cats.syntax.flatMap.toFlatMapOps
import cats.syntax.functor.toFunctorOps
import cats.syntax.traverse.toTraverseOps
import fs2.{Pull, Stream}

import java.time.ZoneId
import scala.concurrent.duration.DurationLong
import scala.jdk.DurationConverters.{JavaDurationOps, ScalaDurationOps}

object tickStream {

  /** Convert a TickStatus into a stream of Tick. For each status:
    *   - Compute the next tick based on the current time
    *   - Sleep for the tick’s snooze duration
    *   - Emit the tick
    */
  def fromTickStatus[F[_]](zeroth: TickStatus[F])(implicit F: Temporal[F]): Stream[F, Tick] =
    Stream.unfoldEval[F, TickStatus[F], Tick](zeroth) { status =>
      F.realTimeInstant.flatMap(status.next).flatMap {
        _.traverse(ns => F.sleep(ns.tick.snooze.toScala).as((ns.tick, ns)))
      }
    }

  /** Stream ticks in a **“sleep first, then emit”** pattern.
    *
    * Each tick represents a structured time-frame governed by a `Policy`.
    *
    * Behavior:
    *   - The **first tick** (index = 1) is **not emitted immediately**; the stream waits for the tick's
    *     `snooze` first.
    *   - After sleeping for `tick#snooze`, the tick is emitted downstream.
    *
    * ===Key semantics===
    *
    *   - `tick.acquires` represents the **moment the downstream actually pull the tick**.
    *   - The stream terminates automatically when the policy has no further decisions (e.g., `GiveUp` or
    *     `Limited` exhausted).
    *
    * @param zoneId
    *   The time zone used for generating ticks
    * @param f
    *   The scheduling/retry policy controlling tick evolution
    * @tparam F
    *   Effect type supporting asynchronous operations (e.g., `IO`)
    * @return
    *   A `Stream[F, Tick]` that emits sequential ticks according to the policy and sleeps first
    */
  def tickScheduled[F[_]: Async](zoneId: ZoneId, f: Policy.type => Policy): Stream[F, Tick] =
    Stream.eval[F, TickStatus[F]](TickStatus.zeroth[F](zoneId, f(Policy))).flatMap(fromTickStatus[F])

  /** Stream ticks starting from the zeroth tick. The first tick (index = 0) is emitted immediately, then
    * continues according to the policy.
    */
  def tickImmediate[F[_]: Async](zoneId: ZoneId, f: Policy.type => Policy): Stream[F, Tick] =
    Stream
      .eval[F, TickStatus[F]](TickStatus.zeroth[F](zoneId, f(Policy)))
      .flatMap(zero => fromTickStatus[F](zero).cons1(zero.tick))

  /** Stream ticks in an **“emit first, then sleep”** pattern.
    *
    * Each tick represents a structured time-frame governed by a `Policy`.
    *
    * Behavior:
    *   - The **first tick** (index = 1) is emitted immediately.
    *   - Each subsequent tick is computed according to the policy sequence.
    *   - After emission, the stream sleeps until the tick’s `conclude` time.
    *
    * ===Key semantics===
    *
    *   - `tick.acquires` represents the **moment the downstream actually pull the tick**.
    *   - Tick emission may **drift relative to the nominal schedule** if processing is slow.
    *   - The stream terminates automatically when the policy has no further decisions (e.g., `GiveUp` or
    *     `Limited` exhausted).
    *
    * @param zoneId
    *   The time zone used for generating ticks
    * @param f
    *   The scheduling/retry policy controlling tick evolution
    * @tparam F
    *   Effect type supporting asynchronous operations (e.g., `IO`)
    * @return
    *   A `Stream[F, Tick]` that emits sequential ticks respecting the policy timing
    */
  def tickFuture[F[_]](zoneId: ZoneId, f: Policy.type => Policy)(implicit F: Async[F]): Stream[F, Tick] =
    Stream.eval[F, TickStatus[F]](TickStatus.zeroth[F](zoneId, f(Policy))).flatMap { initStatus =>
      val loop: TickStatus[F] => Pull[F, Tick, Unit] =
        Pull.loop[F, Tick, TickStatus[F]] { ts =>
          Pull
            .eval(F.realTimeInstant.flatMap(ts.next))
            .flatMap(_.traverse(ns => Pull.output1(ns.tick) >> Pull.sleep(ns.tick.snooze.toScala).as(ns)))
        }

      loop(initStatus).stream
    }

  /** for test Policy only
    */
  def testPolicy[F[_]: Sync](zoneId: ZoneId, f: Policy.type => Policy): Stream[F, Tick] =
    for {
      init <- Stream.eval(TickStatus.zeroth(zoneId, f(Policy)))
      rng <- Stream.eval(Random.scalaUtilRandom[F])
      tick <- Stream.unfoldEval(init) { ts =>
        rng
          .betweenLong(0L, 5L)
          .map(_.milliseconds.toJava)
          .flatMap(jitter => ts.next(ts.tick.conclude.plus(jitter)))
          .map(_.map(s => (s.tick, s)))
      }
    } yield tick

  def testPolicy[F[_]: Sync](f: Policy.type => Policy): Stream[F, Tick] =
    testPolicy[F](ZoneId.systemDefault(), f)
}
