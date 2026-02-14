package com.github.chenharryhua.nanjin.common.chrono

import cats.effect.kernel.{Async, Sync, Temporal}
import cats.effect.std.Random
import cats.syntax.flatMap.toFlatMapOps
import cats.syntax.functor.toFunctorOps
import cats.syntax.traverse.toTraverseOps
import fs2.{Pull, Stream}

import java.time.{Duration, ZoneId}
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

  /** Stream ticks according to a policy in a **“sleep first, then emit”** pattern.
    *
    *   - The **first tick** is **not emitted immediately**; the stream waits for its scheduled snooze before
    *     emitting.
    *   - Each subsequent tick waits for its full snooze duration before being emitted.
    *   - Tick emission strictly respects the **planned snooze intervals** defined by the policy.
    *   - Tick indices increase sequentially according to the policy; if the policy ends (e.g., `GiveUp`), the
    *     stream terminates.
    *   - Downstream processing time does **not shorten the snooze**; ticks are emitted after waiting their
    *     full scheduled interval.
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

  /** Stream ticks according to a policy in an **“emit first, then sleep to next”** pattern.
    *   - The **first tick** is emitted immediately when the stream starts.
    *   - Subsequent ticks are emitted in **policy order** (tick indices 1, 2 …).
    *   - Each tick’s emission respects the policy as closely as possible, **even if downstream processing is
    *     slow**. Downstream work that takes longer than the nominal interval may delay the next observable
    *     tick.
    *   - Tick indices **always increase sequentially**; no tick index is skipped.
    *   - Observed tick times may drift relative to the nominal schedule if downstream is slow
    */
  def tickFuture[F[_]](zoneId: ZoneId, f: Policy.type => Policy)(implicit F: Async[F]): Stream[F, Tick] =
    Stream.eval[F, TickStatus[F]](TickStatus.zeroth[F](zoneId, f(Policy))).flatMap { initStatus =>
      val loop: TickStatus[F] => Pull[F, Tick, Unit] =
        Pull.loop[F, Tick, TickStatus[F]] { ts =>
          Pull.eval(F.realTimeInstant.flatMap(ts.next)).flatMap {
            _.traverse { ns =>
              Pull.output1(ns.tick) >>
                Pull.eval(F.realTimeInstant).flatMap { ts =>
                  if (ts.isBefore(ns.tick.conclude)) {
                    val gap = Duration.between(ts, ns.tick.conclude)
                    Pull.sleep(gap.toScala).as(ns)
                  } else Pull.pure(ns)
                }
            }
          }
        }

      loop(initStatus).stream
    }

  /** for test Policy only
    */
  def testPolicy[F[_]: Sync](f: Policy.type => Policy): Stream[F, Tick] =
    for {
      init <- Stream.eval(TickStatus.zeroth(ZoneId.systemDefault(), f(Policy)))
      rng <- Stream.eval(Random.scalaUtilRandom[F])
      tick <- Stream.unfoldEval(init) { ts =>
        rng
          .betweenLong(0L, 5L)
          .map(_.milliseconds.toJava)
          .flatMap(jitter => ts.next(ts.tick.conclude.plus(jitter)))
          .map(_.map(s => (s.tick, s)))
      }
    } yield tick
}
