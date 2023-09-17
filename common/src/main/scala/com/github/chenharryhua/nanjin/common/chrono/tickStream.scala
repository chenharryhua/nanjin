package com.github.chenharryhua.nanjin.common.chrono

import cats.effect.kernel.Temporal
import cats.effect.std.UUIDGen
import cats.syntax.all.*
import fs2.Stream

import scala.jdk.DurationConverters.JavaDurationOps

object tickStream {
  def apply[F[_]](policy: Policy, initTick: Tick)(implicit F: Temporal[F]): Stream[F, Tick] =
    Stream.unfoldEval[F, Tick, Tick](initTick) { previous =>
      F.realTimeInstant.flatMap { now =>
        policy.decide(previous, now).traverse(tick => F.sleep(tick.snooze.toScala).as((tick, tick)))
      }
    }
  def apply[F[_]: UUIDGen: Temporal](policy: Policy): Stream[F, Tick] =
    Stream.eval[F, Tick](Tick.Zero[F]).flatMap(apply(policy, _))
}
