package com.github.chenharryhua.nanjin.common.policy

import cats.Show
import cats.implicits.{showInterpolator, toShow}
import com.github.chenharryhua.nanjin.common.policy.Policy.{FollowBy, Limited, Repeat}
import cron4s.CronExpr
import cron4s.lib.javatime.javaTemporalInstance
import monocle.syntax.all.*

import java.time.{Duration, Instant, ZoneId}
import scala.jdk.DurationConverters.JavaDurationOps

sealed trait Policy {
  def decide(tick: Tick, now: Instant): Option[Tick]
  def show: String

  final override def toString: String = show
}
sealed trait InfinitePolicy extends Policy {
  final def limited(num: Int): Limited = Limited(this, num)
}
sealed trait FinitePolicy extends Policy {
  final def followBy(other: FinitePolicy): FollowBy = FollowBy(this, other)
  final def repeat: Repeat                          = Repeat(this)
}

object Policy {
  implicit val showPolicy: Show[Policy] = _.show

  case object GiveUp extends Policy {
    override def decide(tick: Tick, now: Instant): Option[Tick] = None

    override def show: String = show"None"
  }

  type GiveUp = GiveUp.type

  final case class Constant(baseDelay: Duration) extends InfinitePolicy {
    override def decide(tick: Tick, now: Instant): Option[Tick] =
      Some(
        Tick(
          sequenceId = tick.sequenceId,
          start = tick.start,
          index = tick.index + 1,
          counter = tick.counter + 1,
          previous = tick.wakeup,
          snooze = baseDelay,
          acquire = now,
          guessNext = Some(now.plus(baseDelay.multipliedBy(2)))
        ))

    override def show: String = show"Constant(${baseDelay.toScala})"
  }

  final case class FixedPace(baseDelay: Duration) extends InfinitePolicy {
    override def decide(tick: Tick, now: Instant): Option[Tick] = {
      val delay = {
        val multi = (Duration.between(tick.start, now).toScala / baseDelay.toScala).ceil.toLong
        Duration.between(now, tick.start.plus(baseDelay.multipliedBy(multi)))
      }
      Some(
        Tick(
          sequenceId = tick.sequenceId,
          start = tick.start,
          index = tick.index + 1,
          counter = tick.counter + 1,
          previous = tick.wakeup,
          snooze = delay,
          acquire = now,
          guessNext = Some(now.plus(delay).plus(baseDelay))
        ))
    }

    override def show: String = show"FixRate(${baseDelay.toScala})"
  }

  final case class Exponential(baseDelay: Duration) extends InfinitePolicy {
    override def decide(tick: Tick, now: Instant): Option[Tick] =
      Some(
        Tick(
          sequenceId = tick.sequenceId,
          start = tick.start,
          index = tick.index + 1,
          counter = tick.counter + 1,
          previous = tick.wakeup,
          snooze = baseDelay.multipliedBy(Math.pow(2, tick.counter.toDouble).toLong),
          acquire = now,
          guessNext = Some(now.plus(baseDelay.multipliedBy(Math.pow(2, tick.counter.toDouble + 1).toLong)))
        ))

    override def show: String = show"Exponential(${baseDelay.toScala})"
  }

  final case class Fibonacci(baseDelay: Duration) extends InfinitePolicy {
    override def decide(tick: Tick, now: Instant): Option[Tick] =
      Some(
        Tick(
          sequenceId = tick.sequenceId,
          start = tick.start,
          index = tick.index + 1,
          counter = tick.counter + 1,
          previous = tick.wakeup,
          snooze = baseDelay.multipliedBy(Fib.fibonacci(tick.counter)),
          acquire = now,
          guessNext = Some(now.plus(baseDelay.multipliedBy(Fib.fibonacci(tick.counter + 1))))
        ))

    override def show: String = show"Fibonacci(${baseDelay.toScala})"
  }

  final case class Crontab(cronExpr: CronExpr, zoneId: ZoneId) extends InfinitePolicy {
    override def decide(tick: Tick, now: Instant): Option[Tick] =
      cronExpr.next(now.atZone(zoneId)).map { zdt =>
        Tick(
          sequenceId = tick.sequenceId,
          start = tick.start,
          index = tick.index + 1,
          counter = tick.counter + 1,
          previous = tick.wakeup,
          snooze = Duration.between(now, zdt),
          acquire = now,
          guessNext = cronExpr.next(zdt).map(_.toInstant)
        )
      }

    override def show: String = show"Crontab(${cronExpr.show})"
  }

  final case class Limited(policy: InfinitePolicy, limit: Int) extends FinitePolicy {
    override def decide(tick: Tick, now: Instant): Option[Tick] =
      policy.decide(tick, now).flatMap(t => if (t.counter <= limit) Some(t) else None)

    override def show: String = show"(${policy.show}+$limit)"
  }

  final case class FollowBy(first: FinitePolicy, second: FinitePolicy) extends FinitePolicy {
    override def decide(tick: Tick, now: Instant): Option[Tick] =
      first.decide(tick, now).orElse(second.decide(tick, now))

    override def show: String = show"${first.show}.followBy${second.show}"
  }

  final case class Repeat(policy: Policy) extends InfinitePolicy {
    override def decide(tick: Tick, now: Instant): Option[Tick] =
      policy.decide(tick, now).orElse(policy.decide(tick.focus(_.counter).replace(0), now))

    override def show: String = show"${policy.show}.repeat"
  }
}
