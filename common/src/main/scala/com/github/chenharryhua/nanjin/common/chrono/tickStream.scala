package com.github.chenharryhua.nanjin.common.chrono

import cats.effect.kernel.Temporal
import cats.effect.std.UUIDGen
import cats.syntax.all.*
import fs2.Stream

import scala.jdk.DurationConverters.JavaDurationOps

object tickStream {
  def apply[F[_]](zeroth: TickStatus)(implicit F: Temporal[F]): Stream[F, Tick] =
    Stream.unfoldEval[F, TickStatus, Tick](zeroth) { status =>
      F.realTimeInstant.flatMap { now =>
        status.next(now).traverse(nt => F.sleep(nt.tick.snooze.toScala).as((nt.tick, nt)))
      }
    }
  def apply[F[_]: UUIDGen: Temporal](policy: Policy): Stream[F, Tick] =
    Stream.eval[F, TickStatus](TickStatus(policy)).flatMap(apply(_))
}
