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
  def fromTickStatus[F[_]](zeroth: PolicyTick[F])(implicit F: Temporal[F]): Stream[F, Tick] =
    Stream.unfoldEval[F, PolicyTick[F], Tick](zeroth) { status =>
      F.realTimeInstant.flatMap(status.next).flatMap {
        _.traverse(ns => F.sleep(ns.tick.snooze.toScala).as((ns.tick, ns)))
      }
    }

  /** Stream ticks according to a policy in a **“sleep first, then emit”** pattern.
    *
    * Stream ticks in a **strict schedule** pattern.
    */
  def tickScheduled[F[_]: Async](zoneId: ZoneId, f: Policy.type => Policy): Stream[F, Tick] =
    Stream.eval[F, PolicyTick[F]](PolicyTick.zeroth[F](zoneId, f(Policy))).flatMap(fromTickStatus[F])

  /** Stream ticks starting from the zeroth tick. The first tick (index = 0) is emitted immediately, then
    * continues according to the policy.
    */
  def tickImmediate[F[_]: Async](zoneId: ZoneId, f: Policy.type => Policy): Stream[F, Tick] =
    Stream
      .eval[F, PolicyTick[F]](PolicyTick.zeroth[F](zoneId, f(Policy)))
      .flatMap(zero => fromTickStatus[F](zero).cons1(zero.tick))

  /** Stream ticks according to a policy in an **“emit first, then sleep to next”** pattern.
    *
    * Stream ticks in an **“at-least schedule”** manner according to the given `Policy`.
    */
  def tickFuture[F[_]](zoneId: ZoneId, f: Policy.type => Policy)(implicit F: Async[F]): Stream[F, Tick] =
    Stream.eval[F, PolicyTick[F]](PolicyTick.zeroth[F](zoneId, f(Policy))).flatMap { initStatus =>
      val loop: PolicyTick[F] => Pull[F, Tick, Unit] =
        Pull.loop[F, Tick, PolicyTick[F]] { ts =>
          Pull.eval(F.realTimeInstant.flatMap(ts.next)).flatMap {
            _.traverse { ns =>
              Pull.output1(ns.tick) >> // downstream process the tick
                Pull.eval(F.realTimeInstant).flatMap { ts =>
                  val conclude = ns.tick.conclude
                  if (ts.isBefore(conclude)) {
                    val gap = Duration.between(ts, conclude)
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
      init <- Stream.eval(PolicyTick.zeroth(ZoneId.systemDefault(), f(Policy)))
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
