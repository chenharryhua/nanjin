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

private object EvalPolicy {
  import PolicyF.*

  /** The first `wakeup + n * delay` that is strictly after `now`, for `n >= 1`.
    *
    * Computed directly rather than by stepping one period at a time. After a long gap, a suspended machine or
    * a stop-the-world pause, a small `delay` would otherwise need `elapsed / delay` iterations, each
    * allocating a `Duration` and an `Instant`, inside a single tick step.
    *
    * `delay` is positive for any policy built through `Policy.fixedRate`, which requires it.
    */
  private def fixed_rate_snooze(wakeup: Instant, now: Instant, delay: Duration): Instant = {
    val elapsed = Duration.between(wakeup, now)
    // `dividedBy` truncates toward zero, which is the floor for a non-negative elapsed. A negative elapsed
    // means `now` precedes `wakeup`, so the first period is already in the future and no periods are past.
    val periods = if (elapsed.isNegative) 0L else elapsed.dividedBy(delay)
    wakeup.plus(delay.multipliedBy(periods + 1L))
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
          val conclude =
            if (tick.index === 0L) now.plus(delay)
            else fixed_rate_snooze(tick.conclude, now, delay)
          tick.nextTick(now, conclude).some.pure[F]
        })

      // ops
      case Limited(policy, limit) => policy.take(limit)

      case FollowedBy(leader, follower) => leader #::: follower

      case Repeat(policy) =>
        if (policy.isEmpty) LazyList.empty
        else LazyList.continually(policy).flatten

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
        def concludes_at_excluded(tick: Tick): Boolean =
          tick.local(_.conclude).toLocalTime === except

        policy.map { stepper =>
          TickStepper { (acq: Acquisition) =>
            stepper(acq).flatMap {
              case Some(tick) =>
                if (concludes_at_excluded(tick))
                  stepper.step(tick, tick.conclude).map(_.flatMap { nt =>
                    val stretched = tick.withSnoozeStretch(nt.snooze)
                    // A local time recurs only once a day, so a second consecutive landing on the excluded
                    // time means this stepper's period is a whole number of days and every later candidate
                    // lands there too. One retry therefore settles it: the exclusion can never be
                    // satisfied, so there is no valid tick and the policy is exhausted.
                    Option.unless(concludes_at_excluded(stretched))(stretched)
                  })
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

    }

  def apply[F[_]: {Random, Monad}](policy: Fix[PolicyF]): LazyList[TickStepper[F]] =
    scheme.cata(algebra(Random[F])).apply(policy)

}
