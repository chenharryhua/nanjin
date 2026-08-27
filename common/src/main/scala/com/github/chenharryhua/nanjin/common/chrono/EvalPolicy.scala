package com.github.chenharryhua.nanjin.common.chrono

import cats.Monad
import cats.effect.std.Random
import cats.syntax.applicative.given
import cats.syntax.apply.given
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import cats.syntax.option.given
import cats.syntax.order.given
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
        LazyList(TickStepper { case Acquisition(tick, now) =>
          cronExpr.next(now.atZone(tick.zoneId)).map(zdt => tick.nextTick(now, zdt.toInstant)).pure[F]
        })

      case FixedDelay(delays) =>
        LazyList.from(delays.toList).map { delay =>
          TickStepper { case Acquisition(tick, now) => tick.nextTick(now, now.plus(delay)).some.pure[F] }
        }

      case FixedRate(delay) =>
        LazyList(TickStepper { case Acquisition(tick, now) =>
          tick.nextTick(now, fixedRateSnooze(tick.conclude, now, delay, 1)).some.pure[F]
        })

      // ops
      case Limited(policy, limit) => policy.take(limit)

      case FollowedBy(leader, follower) => leader #::: follower

      case Repeat(policy) => LazyList.continually(policy).flatten

      case Meet(first, second) =>
        first.zip(second).map { case (sa, sb) =>
          TickStepper { (acq: Acquisition) =>
            (sa(acq), sb(acq)).mapN {
              case (Some(ra), Some(rb)) => Some(if (ra.snooze < rb.snooze) ra else rb)
              case _                    => None
            }
          }
        }

      case Except(policy, except) =>
        policy.map { stepper =>
          TickStepper { (acq: Acquisition) =>
            stepper(acq).flatMap {
              case Some(tick) =>
                if (tick.local(_.conclude).toLocalTime === except)
                  stepper.step(tick, tick.conclude).map(_.map(nt => tick.withSnoozeStretch(nt.snooze)))
                else tick.some.pure[F]
              case None => None.pure[F]
            }
          }
        }

      case Offset(policy, offset) =>
        policy.map { stepper =>
          TickStepper { (acq: Acquisition) =>
            stepper(acq).map(_.map(_.withSnoozeStretch(offset)))
          }
        }

      case Jitter(policy, min, max) =>
        policy.map { stepper =>
          TickStepper { (acq: Acquisition) =>
            rng.betweenLong(min.toNanos, max.toNanos).flatMap { delay =>
              stepper(acq).map(_.map(_.withSnoozeStretch(Duration.of(delay, ChronoUnit.NANOS))))
            }
          }
        }

      case Expire(policy, ttl) =>
        policy.map { stepper =>
          TickStepper { (acq: Acquisition) =>
            val elapsed = Duration.between(acq.tick.launchTime, acq.now)
            if (elapsed.compareTo(ttl) >= 0) None.pure[F]
            else
              stepper(acq).map(_.filter(t => Duration.between(t.launchTime, t.conclude).compareTo(ttl) < 0))
          }
        }
    }

  def apply[F[_]: {Random, Monad}](policy: Fix[PolicyF]): LazyList[TickStepper[F]] =
    scheme.cata(algebra(Random[F])).apply(policy)

}
