package com.github.chenharryhua.nanjin.common.chrono

import cats.effect.std.Random
import cats.syntax.applicative.given
import cats.syntax.apply.given
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import cats.syntax.order.given
import cats.syntax.show.showInterpolator
import cats.{Monad, Show}
import cron4s.lib.javatime.javaTemporalInstance
import cron4s.syntax.all.*
import higherkindness.droste.data.Fix
import higherkindness.droste.{scheme, Algebra}
import org.typelevel.cats.time.instances.all.*

import java.time.temporal.ChronoUnit
import java.time.{Duration, Instant}
import scala.annotation.tailrec

private object EvalPolicy {
  import PolicyF.*

  @tailrec
  private def fixedRateSnooze(wakeup: Instant, now: Instant, delay: Duration, count: Long): Instant = {
    val next = wakeup.plus(delay.multipliedBy(count))
    if (next.isAfter(now)) next
    else
      fixedRateSnooze(wakeup, now, delay, count + 1)
  }

  private def algebra[F[_]: Monad](rng: Random[F]): Algebra[PolicyF, LazyList[TickStepper[F]]] =
    Algebra[PolicyF, LazyList[TickStepper[F]]] {

      case Empty() => LazyList.empty

      case Crontab(cronExpr) =>
        val seed: TickStepper[F] = TickStepper { case TickRequest(tick, now) =>
          cronExpr.next(now.atZone(tick.zoneId)) match {
            case Some(value) => tick.nextTick(now, value.toInstant).pure[F]
            case None        => // should not happen
              sys.error(show"$cronExpr returned None at $now. This should never happen.")
          }
        }
        LazyList.continually(seed)

      case FixedDelay(delays) =>
        val seed: LazyList[TickStepper[F]] = LazyList.from(delays.toList).map[TickStepper[F]] { delay =>
          TickStepper { case TickRequest(tick, now) => tick.nextTick(now, now.plus(delay)).pure[F] }
        }
        LazyList.continually(seed).flatten

      case FixedRate(delay) =>
        LazyList.continually(TickStepper { case TickRequest(tick, now) =>
          tick.nextTick(now, fixedRateSnooze(tick.conclude, now, delay, 1)).pure[F]
        })

      // ops
      case Limited(policy, limit) => policy.take(limit)

      case FollowedBy(leader, follower) => leader #::: follower

      case Repeat(policy) => LazyList.continually(policy).flatten

      // https://en.wikipedia.org/wiki/Join_and_meet
      case Meet(first, second) =>
        first.zip(second).map { case (sa: TickStepper[F], sb: TickStepper[F]) =>
          TickStepper { (req: TickRequest) =>
            (sa(req), sb(req)).mapN { (ra, rb) =>
              if (ra.snooze < rb.snooze) ra else rb // shorter win
            }
          }
        }

      case Except(policy, except) =>
        policy.map { (stepper: TickStepper[F]) =>
          TickStepper { (req: TickRequest) =>
            stepper(req).flatMap { tick =>
              if (tick.local(_.conclude).toLocalTime === except) {
                stepper.step(tick, tick.conclude).map(nt => tick.withSnoozeStretch(nt.snooze))
              } else tick.pure[F]
            }
          }
        }

      case Offset(policy, offset) =>
        policy.map { (stepper: TickStepper[F]) =>
          TickStepper { (req: TickRequest) =>
            stepper(req).map(_.withSnoozeStretch(offset))
          }
        }

      case Jitter(policy, min, max) =>
        policy.map { (stepper: TickStepper[F]) =>
          TickStepper { (req: TickRequest) =>
            rng.betweenLong(min.toNanos, max.toNanos).flatMap { delay =>
              stepper(req).map(_.withSnoozeStretch(Duration.of(delay, ChronoUnit.NANOS)))
            }
          }
        }
    }

  def apply[F[_]: {Random, Monad}](policy: Fix[PolicyF]): LazyList[TickStepper[F]] =
    scheme.cata(algebra(Random[F])).apply(policy)

}
