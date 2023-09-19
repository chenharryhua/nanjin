package com.github.chenharryhua.nanjin.common.chrono

import cats.Show
import cats.implicits.{showInterpolator, toShow}
import com.github.chenharryhua.nanjin.common.DurationFormatter
import com.github.chenharryhua.nanjin.common.chrono.Policy.{Capped, FollowedBy, Limited, Repeat}
import cron4s.CronExpr
import cron4s.lib.javatime.javaTemporalInstance

import java.time.temporal.ChronoUnit
import java.time.{Duration, Instant, ZoneId}
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.{JavaDurationOps, ScalaDurationOps}
import scala.util.Random

sealed trait Manipulation
private object Manipulation {
  case object ResetCounter extends Manipulation
  case object DoNothing extends Manipulation
}

sealed trait Policy extends Serializable with Product {
  def decisions: LazyList[(TickStatus, Instant) => Either[Manipulation, Duration]]

  def show: String

  final override def toString: String           = show
  final def limited(num: Int): Policy           = Limited(this, num)
  final def followedBy(other: Policy): Policy   = FollowedBy(this, other)
  final def repeat: Policy                      = Repeat(this)
  final def capped(cap: Duration): Policy       = Capped(this, cap)
  final def capped(cap: FiniteDuration): Policy = Capped(this, cap.toJava)
}

private object Policy {
  implicit val showPolicy: Show[Policy] = _.show
  private val fmt: DurationFormatter    = DurationFormatter.defaultFormatter

  case object GiveUp extends Policy {
    override val decisions: LazyList[(TickStatus, Instant) => Either[Manipulation, Duration]] = LazyList.empty

    override def show: String = show"GiveUp"
  }

  final case class Constant(baseDelay: Duration) extends Policy {
    private val delay: (TickStatus, Instant) => Either[Manipulation, Duration] =
      (_, _) => Right(baseDelay)

    override val decisions: LazyList[(TickStatus, Instant) => Either[Manipulation, Duration]] =
      LazyList.continually(delay)

    override def show: String = show"Constant(${fmt.format(baseDelay)})"
  }

  final case class FixedPace(baseDelay: Duration) extends Policy {
    private val delay: (TickStatus, Instant) => Either[Manipulation, Duration] =
      (tick, now) =>
        Right {
          val multi = (Duration.between(tick.tick.launchTime, now).toScala / baseDelay.toScala).ceil.toLong
          Duration.between(now, tick.tick.launchTime.plus(baseDelay.multipliedBy(multi)))
        }

    override val decisions: LazyList[(TickStatus, Instant) => Either[Manipulation, Duration]] =
      LazyList.continually(delay)

    override def show: String = show"FixRate(${fmt.format(baseDelay)})"
  }

  final case class Exponential(baseDelay: Duration) extends Policy {
    private val delay: (TickStatus, Instant) => Either[Manipulation, Duration] =
      (tick, _) => Right(baseDelay.multipliedBy(Math.pow(2, tick.counter.toDouble).toLong))

    override val decisions: LazyList[(TickStatus, Instant) => Either[Manipulation, Duration]] =
      LazyList.continually(delay)

    override def show: String = show"Exponential(${fmt.format(baseDelay)})"
  }

  final case class Fibonacci(baseDelay: Duration) extends Policy {
    private val delay: (TickStatus, Instant) => Either[Manipulation, Duration] =
      (tick, _) => Right(baseDelay.multipliedBy(Fib.fibonacci(tick.counter + 1)))

    override val decisions: LazyList[(TickStatus, Instant) => Either[Manipulation, Duration]] =
      LazyList.continually(delay)

    override def show: String = show"Fibonacci(${fmt.format(baseDelay)})"
  }

  final case class Crontab(cronExpr: CronExpr, zoneId: ZoneId) extends Policy {
    private val delay: (TickStatus, Instant) => Either[Manipulation, Duration] = (_, now) =>
      cronExpr.next(now.atZone(zoneId)).map(zdt => Duration.between(now, zdt)) match {
        case Some(value) => Right(value)
        case None        => Left(Manipulation.DoNothing)
      }

    override val decisions: LazyList[(TickStatus, Instant) => Either[Manipulation, Duration]] =
      LazyList.continually(delay)

    override def show: String = show"Cron(${cronExpr.show})"
  }

  final case class Jitter(min: Duration, max: Duration) extends Policy {
    private val delay: (TickStatus, Instant) => Either[Manipulation, Duration] =
      (_, _) => Right(Duration.of(Random.between(min.toNanos, max.toNanos), ChronoUnit.NANOS))

    override val decisions: LazyList[(TickStatus, Instant) => Either[Manipulation, Duration]] =
      LazyList.continually(delay)

    override def show: String = show"Jitter(${fmt.format(min)},${fmt.format(max)})"
  }

  private val resetCounter: (TickStatus, Instant) => Either[Manipulation, Duration] =
    (_, _) => Left(Manipulation.ResetCounter)

  final case class Limited(policy: Policy, limit: Int) extends Policy {
    override val decisions: LazyList[(TickStatus, Instant) => Either[Manipulation, Duration]] =
      policy.decisions.take(limit)

    override def show: String = show"(${policy.show}+$limit)"
  }

  final case class FollowedBy(first: Policy, second: Policy) extends Policy {

    override val decisions: LazyList[(TickStatus, Instant) => Either[Manipulation, Duration]] =
      first.decisions #::: (resetCounter #:: second.decisions)

    override def show: String = show"${first.show}.followBy(${second.show})"
  }

  final case class Repeat(policy: Policy) extends Policy {

    override val decisions: LazyList[(TickStatus, Instant) => Either[Manipulation, Duration]] =
      policy.decisions #::: (resetCounter #:: decisions)

    override def show: String = show"${policy.show}.repeat"
  }

  final case class Capped(policy: Policy, cap: Duration) extends Policy {

    override def decisions: LazyList[(TickStatus, Instant) => Either[Manipulation, Duration]] =
      policy.decisions.map { f =>
        val g: ((TickStatus, Instant)) => Either[Manipulation, Duration] =
          f.tupled.andThen(_.map(d => if (d.compareTo(cap) < 0) d else cap))
        (t: TickStatus, i: Instant) => g((t, i))
      }

    override def show: String = show"${policy.show}.capped(${fmt.format(cap)})"
  }
}
