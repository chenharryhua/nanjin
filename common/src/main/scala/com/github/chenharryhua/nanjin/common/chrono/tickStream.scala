package com.github.chenharryhua.nanjin.common.chrono

import cats.effect.kernel.{Async, Temporal}
import cats.syntax.all.*
import fs2.Stream

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

  /** One based tick stream - tick index start from one
    */
  def fromOne[F[_]: Async](policy: Policy, zoneId: ZoneId): Stream[F, Tick] =
    Stream.eval[F, TickStatus](TickStatus.zeroth[F](policy, zoneId)).flatMap(fromTickStatus[F])

  /** Zero based tick stream - tick index start from zero
    */
  def fromZero[F[_]: Async](policy: Policy, zoneId: ZoneId): Stream[F, Tick] =
    Stream.eval(TickStatus.zeroth(policy, zoneId)).flatMap(ts => fromTickStatus(ts).cons1(ts.tick))
}

/** for testing
  */
object tickLazyList {
  def fromTickStatus(init: TickStatus): LazyList[Tick] =
    LazyList.unfold(init)(ts =>
      ts.next(ts.tick.wakeup.plus(Random.between(1, 5).milliseconds.toJava)).map(s => (s.tick, s)))

  def fromOne(policy: Policy, zoneId: ZoneId): LazyList[Tick] =
    fromTickStatus(TickStatus(Tick.zeroth(UUID.randomUUID(), zoneId, Instant.now())).renewPolicy(policy))

  def fromOne(policy: Policy): LazyList[Tick] =
    fromOne(policy, ZoneId.systemDefault())
}
