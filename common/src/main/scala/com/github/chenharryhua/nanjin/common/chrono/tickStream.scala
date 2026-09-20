package com.github.chenharryhua.nanjin.common.chrono

import cats.effect.kernel.{Async, Sync}
import cats.effect.std.Random
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import cats.syntax.traverse.given
import fs2.{Pull, Stream}

import java.time.{Duration, ZoneId}
import scala.concurrent.duration.DurationLong
import scala.jdk.DurationConverters.{JavaDurationOps, ScalaDurationOps}

object tickStream {

  private def seedPolicyTick[F[_]: Async](
    zoneId: ZoneId,
    f: Policy.type => Policy): Stream[F, PolicyTick[F]] =
    Stream.eval(PolicyTick.seed[F](zoneId, f(Policy)))

  /** Stream ticks according to a policy in a **“sleep first, then emit”** pattern.
    *
    * The stream follows a strict schedule: each tick is emitted after waiting for the current tick's snooze
    * duration.
    */
  def tickScheduled[F[_]](zoneId: ZoneId, f: Policy.type => Policy)(using F: Async[F]): Stream[F, Tick] =
    seedPolicyTick(zoneId, f).flatMap { seed =>
      Stream.unfoldEval[F, PolicyTick[F], Tick](seed) { policyTick =>
        F.realTimeInstant.flatMap(policyTick.next).flatMap {
          _.traverse(ns => F.sleep(ns.tick.snooze.toScala).as((ns.tick, ns)))
        }
      }
    }

  /** Stream ticks according to a policy in an **“emit first, then sleep to next”** pattern.
    *
    * This is an at-least schedule: if downstream work overruns the requested cadence, the stream emits the
    * next tick as soon as it can and then sleeps only for the remaining gap.
    */
  def tickFuture[F[_]](zoneId: ZoneId, f: Policy.type => Policy)(using F: Async[F]): Stream[F, Tick] =
    seedPolicyTick(zoneId, f).flatMap { seed =>
      val loop: PolicyTick[F] => Pull[F, Tick, Unit] =
        Pull.loop[F, Tick, PolicyTick[F]] { policyTick =>
          Pull.eval(F.realTimeInstant.flatMap(policyTick.next)).flatMap {
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

      loop(seed).stream
    }

  /** Public helper for observing policy progression without sleeping the wall clock.
    *
    * A small random jitter is applied when querying the next tick, making it useful for tests, demos, and
    * debugging timing behavior.
    */
  def testPolicy[F[_]: Sync](f: Policy.type => Policy): Stream[F, Tick] =
    for {
      seed <- Stream.eval(PolicyTick.seed(ZoneId.systemDefault(), f(Policy)))
      rng <- Stream.eval(Random.scalaUtilRandom[F])
      tick <- Stream.unfoldEval(seed) { policyTick =>
        rng
          .betweenLong(0L, 5L)
          .map(_.milliseconds.toJava)
          .flatMap(jitter => policyTick.next(policyTick.tick.conclude.plus(jitter)))
          .map(_.map(s => (s.tick, s)))
      }
    } yield tick
}
