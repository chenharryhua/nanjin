package com.github.chenharryhua.nanjin.common.chrono

import cats.effect.kernel.Temporal
import cats.effect.std.UUIDGen
import cats.syntax.all.*
import fs2.Stream

import java.time.ZoneId
import scala.concurrent.duration.DurationInt
import scala.jdk.DurationConverters.{JavaDurationOps, ScalaDurationOps}
import scala.util.Random

object tickStream {
  def apply[F[_]](zeroth: TickStatus)(implicit F: Temporal[F]): Stream[F, Tick] =
    Stream.unfoldEval[F, TickStatus, Tick](zeroth) { status =>
      F.realTimeInstant.flatMap { now =>
        status.next(now).traverse(nt => F.sleep(nt.tick.snooze.toScala).as((nt.tick, nt)))
      }
    }

  def apply[F[_]: UUIDGen: Temporal](policy: Policy, zoneId: ZoneId): Stream[F, Tick] =
    Stream.eval[F, TickStatus](TickStatus.zeroth[F](policy, zoneId)).flatMap(apply[F])
}

object lazyTickList {
  def apply(init: TickStatus): LazyList[Tick] =
    LazyList.unfold(init)(ts =>
      ts.next(ts.tick.wakeup.plus(Random.between(1, 5).milliseconds.toJava)).map(s => (s.tick, s)))
}
