package com.github.chenharryhua.nanjin.guard.service

import cats.effect.Temporal
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.event.Tick
import fs2.Stream
import retry.{PolicyDecision, RetryPolicy, RetryStatus}

import scala.jdk.DurationConverters.ScalaDurationOps

private object awakeEvery {
  final def apply[F[_]](policy: RetryPolicy[F])(implicit F: Temporal[F]): Stream[F, Tick] =
    Stream.unfoldEval[F, RetryStatus, Tick](RetryStatus.NoRetriesYet) { status =>
      for {
        start <- F.realTimeInstant
        next <- policy.decideNextRetry(status).flatMap[Option[(Tick, RetryStatus)]] {
          case PolicyDecision.GiveUp => F.pure(None)
          case PolicyDecision.DelayAndRetry(delay) =>
            F.sleep(delay) >> F.realTimeInstant.map { wakeup =>
              val rs: RetryStatus = status.addRetry(delay)
              val tick: Tick =
                Tick(
                  index = rs.retriesSoFar,
                  pullTime = start,
                  wakeTime = wakeup,
                  delay = delay.toJava
                )
              (tick, rs).some
            }
        }
      } yield next
    }
}
