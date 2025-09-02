package com.github.chenharryhua.nanjin.common.chrono

import cats.effect.kernel.{Async, Temporal}
import cats.syntax.all.*
import fs2.{Pull, Stream}

import java.time.{Instant, ZoneId}
import java.util.UUID
import scala.concurrent.duration.DurationInt
import scala.jdk.DurationConverters.{JavaDurationOps, ScalaDurationOps}
import scala.util.Random

object tickStream {
  def fromTickStatus[F[_]](zeroth: TickStatus)(implicit F: Temporal[F]): Stream[F, Tick] =
    Stream.unfoldEval[F, TickStatus, Tick](zeroth) { status =>
      F.realTimeInstant.flatMap { now =>
        status.next(now).traverse(nt => F.sleep(nt.tick.snooze.toScala).as((nt.tick, nt)))
      }
    }

  /** sleep then emit, so that conclude time of the tick is in the past
    *
    * first tick, which index is One, is NOT emitted immediately.
    */
  def tickScheduled[F[_]: Async](zoneId: ZoneId, policy: Policy): Stream[F, Tick] =
    Stream.eval[F, TickStatus](TickStatus.zeroth[F](zoneId, policy)).flatMap(fromTickStatus[F])

  /** first tick, which index is Zero, is emitted immediately
    */
  def tickImmediate[F[_]: Async](zoneId: ZoneId, policy: Policy): Stream[F, Tick] =
    Stream
      .eval[F, TickStatus](TickStatus.zeroth(zoneId, policy))
      .flatMap(zero => fromTickStatus(zero).cons1(zero.tick))

  /** emit then sleep, so that conclude time of the tick is in the future
    *
    * first tick, which index is One, is immediately emitted
    */
  def tickFuture[F[_]](zoneId: ZoneId, policy: Policy)(implicit F: Async[F]): Stream[F, Tick] =
    Stream.eval[F, TickStatus](TickStatus.zeroth[F](zoneId, policy)).flatMap { status =>
      def go(ticks: Stream[F, Tick]): Pull[F, Tick, Unit] =
        ticks.pull.uncons1.flatMap {
          case Some((tick, rest)) => Pull.output1(tick) >> Pull.sleep(tick.snooze.toScala) >> go(rest)
          case None               => Pull.done
        }
      val sts: Stream[F, Tick] =
        Stream.unfoldEval(status)(s => F.realTimeInstant.map(s.next(_).map(ns => (ns.tick, ns))))

      go(sts).stream
    }
}

/** for testing
  */
object tickLazyList {
  def fromTickStatus(init: TickStatus): LazyList[Tick] =
    LazyList.unfold(init)(ts =>
      ts.next(ts.tick.conclude.plus(Random.between(1, 5).milliseconds.toJava)).map(s => (s.tick, s)))

  def from(zoneId: ZoneId, policy: Policy): LazyList[Tick] =
    fromTickStatus(TickStatus(Tick.zeroth(UUID.randomUUID(), zoneId, Instant.now())).renewPolicy(policy))

  def from(policy: Policy): LazyList[Tick] =
    from(ZoneId.systemDefault(), policy)
}
