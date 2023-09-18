package com.github.chenharryhua.nanjin.common.chrono

import cats.Show
import cats.implicits.{showInterpolator, toShow}
import com.github.chenharryhua.nanjin.common.DurationFormatter
import com.github.chenharryhua.nanjin.common.chrono.Policy.{FollowBy, Limited, Repeat}
import cron4s.CronExpr
import cron4s.lib.javatime.javaTemporalInstance
import monocle.syntax.all.*

import java.time.temporal.ChronoUnit
import java.time.{Duration, Instant, ZoneId}
import scala.jdk.DurationConverters.JavaDurationOps
import scala.util.Random

sealed trait Policy extends Serializable with Product {
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
  private val fmt: DurationFormatter    = DurationFormatter.defaultFormatter

  case object GiveUp extends Policy {
    override def decide(tick: Tick, now: Instant): Option[Tick] = None

    override def show: String = show"GiveUp"
  }

  type GiveUp = GiveUp.type

  final case class Constant(baseDelay: Duration) extends InfinitePolicy {
    override def decide(tick: Tick, now: Instant): Option[Tick] =
      Some(tick.newTick(now, baseDelay))

    override def show: String = show"Constant(${fmt.format(baseDelay)})"
  }

  final case class FixedPace(baseDelay: Duration) extends InfinitePolicy {
    override def decide(tick: Tick, now: Instant): Option[Tick] = {
      val delay = {
        val multi = (Duration.between(tick.launchTime, now).toScala / baseDelay.toScala).ceil.toLong
        Duration.between(now, tick.launchTime.plus(baseDelay.multipliedBy(multi)))
      }
      Some(tick.newTick(now, delay))
    }

    override def show: String = show"FixRate(${fmt.format(baseDelay)})"
  }

  final case class Exponential(baseDelay: Duration) extends InfinitePolicy {
    override def decide(tick: Tick, now: Instant): Option[Tick] =
      Some(tick.newTick(now, baseDelay.multipliedBy(Math.pow(2, tick.counter.toDouble).toLong)))

    override def show: String = show"Exponential(${fmt.format(baseDelay)})"
  }

  final case class Fibonacci(baseDelay: Duration) extends InfinitePolicy {
    override def decide(tick: Tick, now: Instant): Option[Tick] =
      Some(tick.newTick(now, baseDelay.multipliedBy(Fib.fibonacci(tick.counter + 1))))

    override def show: String = show"Fibonacci(${fmt.format(baseDelay)})"
  }

  final case class Crontab(cronExpr: CronExpr, zoneId: ZoneId) extends InfinitePolicy {
    override def decide(tick: Tick, now: Instant): Option[Tick] =
      cronExpr.next(now.atZone(zoneId)).map { zdt =>
        tick.newTick(now, Duration.between(now, zdt))
      }

    override def show: String = show"Cron(${cronExpr.show})"
  }

  final case class Jitter(min: Duration, max: Duration) extends InfinitePolicy {
    override def decide(tick: Tick, now: Instant): Option[Tick] = {
      val delay = Duration.of(Random.between(min.toNanos, max.toNanos), ChronoUnit.NANOS)
      Some(tick.newTick(now, delay))
    }

    override def show: String = show"Jitter(${fmt.format(min)},${fmt.format(max)})"
  }

  final case class Limited(policy: InfinitePolicy, limit: Int) extends FinitePolicy {
    override def decide(tick: Tick, now: Instant): Option[Tick] =
      policy.decide(tick, now).flatMap(nt => if (nt.counter <= limit) Some(nt) else None)

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
