package com.github.chenharryhua.nanjin.common.chrono

import cats.effect.kernel.{Async, Temporal}
import cats.implicits.{toFlatMapOps, toFunctorOps, toTraverseOps}
import fs2.{Pull, Stream}

import java.time.{Instant, ZoneId}
import java.util.UUID
import scala.concurrent.duration.DurationInt
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

  /** Stream ticks according to the policy, sleeping first. The first tick (index = 1) is not emitted
    * immediately.
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

  /** Stream ticks in “emit first, then sleep” mode. The first tick (index = 1) is emitted immediately, and
    * each tick sleeps after emission.
    */
  def tickFuture[F[_]](zoneId: ZoneId, policy: Policy)(implicit F: Async[F]): Stream[F, Tick] =
    Stream.eval[F, TickStatus](TickStatus.zeroth[F](zoneId, policy)).flatMap { status =>
      def go(ticks: Stream[F, Tick]): Pull[F, Tick, Unit] =
        ticks.pull.uncons1.flatMap {
          case Some((tick, rest)) =>
            Pull.output1[F, Tick](tick) >> Pull.sleep[F](tick.snooze.toScala) >> go(rest)
          case None => Pull.done
        }
      val sts: Stream[F, Tick] =
        Stream.unfoldEval[F, TickStatus, Tick](status)(s =>
          F.realTimeInstant.map(s.next(_).map(ns => (ns.tick, ns))))

      go(sts).stream
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
