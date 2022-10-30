package com.github.chenharryhua.nanjin

import cats.effect.Temporal
import cats.syntax.all.*
import fs2.{Pull, Stream}
import retry.{PolicyDecision, RetryPolicy, RetryStatus}

import scala.concurrent.duration.FiniteDuration

package object guard {

  def awakeEvery[F[_]](policy: RetryPolicy[F])(implicit F: Temporal[F]): Stream[F, Int] = {
    def go(status: RetryStatus, wakeup: FiniteDuration): Pull[F, Int, Unit] =
      Pull.eval(F.monotonic).flatMap { now =>
        if (now < wakeup) // sleep and do nothing
          {
            val delay: FiniteDuration = wakeup - now
            Pull.sleep(delay) >> go(status.addRetry(delay), wakeup)
          } else // emit a tick
          Pull.eval(policy.decideNextRetry(status)).flatMap {
            case PolicyDecision.GiveUp => Pull.done
            case PolicyDecision.DelayAndRetry(delay) =>
              Pull.output1(status.retriesSoFar) >>
                Pull.sleep(delay) >>
                go(status.addRetry(delay), now + delay)
          }
      }

    /*
     * ---------------------- 0 ------- 1 -------- 2 ------
     *
     * now---preSchedule-----tick ...  tick  ...  tick ...
     */
    val init: F[Stream[F, Int]] = for {
      now <- F.monotonic
      preSchedule <- policy.decideNextRetry(RetryStatus.NoRetriesYet)
    } yield preSchedule match {
      case PolicyDecision.GiveUp => Stream.empty
      case PolicyDecision.DelayAndRetry(delay) =>
        go(RetryStatus.NoRetriesYet, now + delay).stream
    }

    Stream.force(init)
  }
}
