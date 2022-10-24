package com.github.chenharryhua.nanjin

import cats.effect.Temporal
import cats.syntax.all.*
import fs2.{Pull, Stream}
import retry.{PolicyDecision, RetryPolicy, RetryStatus}

import java.time.{Duration, Instant}
import scala.jdk.DurationConverters.{JavaDurationOps, ScalaDurationOps}

package object guard {

  def awakeEvery[F[_]](policy: RetryPolicy[F])(implicit F: Temporal[F]): Stream[F, Int] = {
    def go(status: RetryStatus, wakeup: Instant): Pull[F, Int, Unit] =
      Pull.eval(F.realTimeInstant).flatMap { now =>
        if (now.isBefore(wakeup)) // sleep and do nothing
          Pull.sleep(Duration.between(now, wakeup).toScala) >> go(status, wakeup)
        else // emit a tick
          Pull.eval(policy.decideNextRetry(status)).flatMap {
            case PolicyDecision.GiveUp => Pull.done
            case PolicyDecision.DelayAndRetry(delay) =>
              Pull.output1(status.retriesSoFar) >>
                Pull.sleep(delay) >>
                go(status.addRetry(delay), now.plus(delay.toJava))
          }
      }

    /** now --------------- 0 ------- 1 -------- 2 ------ ticks
      *
      * ---preSchedule-----tick------tick-------tick-----
      */
    val init: F[Stream[F, Int]] = for {
      now <- F.realTimeInstant
      preSchedule <- policy.decideNextRetry(RetryStatus.NoRetriesYet)
    } yield preSchedule match {
      case PolicyDecision.GiveUp => Stream.empty
      case PolicyDecision.DelayAndRetry(delay) =>
        go(RetryStatus.NoRetriesYet, now.plus(delay.toJava)).stream
    }

    Stream.force(init)
  }
}
