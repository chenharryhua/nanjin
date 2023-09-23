package com.github.chenharryhua.nanjin.common.chrono

import cats.data.NonEmptyList
import cats.implicits.{catsSyntaxEq, showInterpolator}
import cats.{Functor, Show}
import com.github.chenharryhua.nanjin.common.DurationFormatter
import cron4s.CronExpr
import cron4s.lib.javatime.javaTemporalInstance
import higherkindness.droste.data.Fix
import higherkindness.droste.{scheme, Algebra}
import org.typelevel.cats.time.instances.{duration, localdate, localtime}

import java.time.*
import java.time.temporal.ChronoUnit
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.{JavaDurationOps, ScalaDurationOps}
import scala.util.Random

sealed trait PolicyF[K]

private object PolicyF extends localtime with localdate with duration {

  implicit val functorPolicyF: Functor[PolicyF] = cats.derived.semiauto.functor[PolicyF]

  final case class GiveUp[K]() extends PolicyF[K]
  final case class Accordance[K](policy: K) extends PolicyF[K]
  final case class Constant[K](base: Duration) extends PolicyF[K]
  final case class FixedPace[K](base: Duration) extends PolicyF[K]
  final case class Exponential[K](base: Duration, length: Int) extends PolicyF[K]
  final case class Fibonacci[K](base: Duration, length: Int) extends PolicyF[K]
  final case class Crontab[K](cronExpr: CronExpr) extends PolicyF[K]
  final case class Jitter[K](min: Duration, max: Duration) extends PolicyF[K]
  final case class Delays[K](delays: NonEmptyList[Duration]) extends PolicyF[K]

  final case class Limited[K](policy: K, limit: Int) extends PolicyF[K]
  final case class FollowedBy[K](first: K, second: K) extends PolicyF[K]
  final case class Repeat[K](policy: Fix[PolicyF]) extends PolicyF[K]
  final case class EndUp[K](policy: K, endUp: LocalTime) extends PolicyF[K]

  private type CalcTick = TickRequest => Option[Tick]
  final case class TickRequest(tick: Tick, now: Instant)

  private def algebra(zoneId: ZoneId): Algebra[PolicyF, LazyList[CalcTick]] =
    Algebra[PolicyF, LazyList[CalcTick]] {

      case GiveUp() => LazyList.empty

      case Accordance(policy) => policy

      case Constant(base) =>
        LazyList.continually(req => Some(req.tick.newTick(req.now, base)))

      case FixedPace(base) =>
        val calcTick: CalcTick = { case TickRequest(tick, now) =>
          val multi = (Duration.between(tick.launchTime, now).toScala / base.toScala).ceil.toLong
          val gap   = Duration.between(now, tick.launchTime.plus(base.multipliedBy(multi)))
          if (gap === Duration.ZERO) {
            val gap = Duration.between(now, tick.launchTime.plus(base.multipliedBy(multi + 1)))
            Some(tick.newTick(now, gap))
          } else Some(tick.newTick(now, gap))
        }
        LazyList.continually(calcTick)

      case Exponential(base, length) =>
        LazyList.unfold[CalcTick, Int](0) { counter =>
          val calcTick: CalcTick = { req =>
            val delay = base.multipliedBy(Math.pow(2, counter.toDouble).toLong)
            Some(req.tick.newTick(req.now, delay))
          }

          if (counter < length - 1)
            Some((calcTick, counter + 1))
          else
            Some((calcTick, 0))
        }

      case Fibonacci(base, length) =>
        LazyList.unfold[CalcTick, Int](1) { counter =>
          val calcTick: CalcTick =
            req => Some(req.tick.newTick(req.now, base.multipliedBy(Fib.fibonacci(counter))))

          if (counter < length)
            Some((calcTick, counter + 1))
          else
            Some((calcTick, 1))
        }

      case Crontab(cronExpr) =>
        val calcTick: CalcTick = { case TickRequest(tick, now) =>
          cronExpr.next(now.atZone(zoneId)).map(zdt => Duration.between(now, zdt)).map(tick.newTick(now, _))
        }
        LazyList.continually(calcTick)

      case Jitter(min, max) =>
        val calcTick: CalcTick = { req =>
          Some(
            req.tick
              .newTick(req.now, Duration.of(Random.between(min.toNanos, max.toNanos), ChronoUnit.NANOS)))
        }
        LazyList.continually(calcTick)

      case Delays(delays) =>
        LazyList.from(delays.toList.map { delay =>
          { case TickRequest(tick, now) => Some(tick.newTick(now, delay)) }
        })

      // ops
      case Limited(policy, limit) => policy.take(limit)

      case FollowedBy(first, second) => first #::: second

      case Repeat(policy) =>
        LazyList.continually(decisions(policy, zoneId)).flatten

      case EndUp(policy, endUp) =>
        val timeFrame: LazyList[Unit] = LazyList.unfold(ZonedDateTime.now(zoneId)) { prev =>
          val now     = ZonedDateTime.now(zoneId)
          val sameDay = now.toLocalDate === prev.toLocalDate
          val endTime = endUp.atDate(now.toLocalDate).atZone(zoneId)
          if (endTime.isAfter(now) && sameDay) Some(((), now)) else None
        }
        policy.zip(timeFrame).map(_._1)
    }

  private val fmt: DurationFormatter = DurationFormatter.defaultFormatter

  val show: Algebra[PolicyF, String] = Algebra[PolicyF, String] {
    case GiveUp()                  => show"giveUp"
    case Accordance(policy)        => show"accordance($policy)"
    case Constant(base)            => show"constant(${fmt.format(base)})"
    case FixedPace(base)           => show"fixedPace(${fmt.format(base)})"
    case Exponential(base, length) => show"exponential(${fmt.format(base)}, $length)"
    case Fibonacci(base, length)   => show"fibonacci(${fmt.format(base)}, $length)"
    case Crontab(cronExpr)         => show"crontab($cronExpr)"
    case Jitter(min, max)          => show"jitter(${fmt.format(min)}, ${fmt.format(max)})"
    case Delays(delays)            => show"delays(${fmt.format(delays.head)}, ...)"
    // ops
    case Limited(policy, limit)    => show"$policy.limited($limit)"
    case FollowedBy(first, second) => show"$first.followedBy($second)"
    case Repeat(policy)            => show"${Policy(policy)}.repeat"
    case EndUp(policy, endUp)      => show"$policy.endUp($endUp)"
  }

  def decisions(policy: Fix[PolicyF], zoneId: ZoneId): LazyList[CalcTick] =
    scheme.cata(algebra(zoneId)).apply(policy)
}

final case class Policy(policy: Fix[PolicyF]) extends AnyVal {
  import PolicyF.{EndUp, FollowedBy, Limited, Repeat}
  override def toString: String = scheme.cata(PolicyF.show).apply(policy)

  def limited(num: Int): Policy         = Policy(Fix(Limited(policy, num)))
  def followedBy(other: Policy): Policy = Policy(Fix(FollowedBy(policy, other.policy)))
  def repeat: Policy                    = Policy(Fix(Repeat(policy)))

  def endUp(localTime: LocalTime): Policy = Policy(Fix(EndUp(policy, localTime)))
  def endOfDay: Policy                    = endUp(LocalTime.MAX)
}

object Policy {
  implicit val showPolicy: Show[Policy] = _.toString
}

object policies {
  import PolicyF.{Accordance, Constant, Crontab, Delays, Exponential, Fibonacci, FixedPace, GiveUp, Jitter}

  def accordance(policy: Policy): Policy = Policy(Fix(Accordance(policy.policy)))

  def constant(base: Duration): Policy       = Policy(Fix(Constant(base)))
  def constant(base: FiniteDuration): Policy = constant(base.toJava)

  def fixedPace(base: Duration): Policy       = Policy(Fix(FixedPace(base)))
  def fixedPace(base: FiniteDuration): Policy = fixedPace(base.toJava)

  def exponential(base: Duration, length: Int): Policy     = Policy(Fix(Exponential(base, length)))
  def exponential(base: FiniteDuration, expo: Int): Policy = exponential(base.toJava, expo)

  def fibonacci(base: Duration, length: Int): Policy    = Policy(Fix(Fibonacci(base, length)))
  def fibonacci(base: FiniteDuration, max: Int): Policy = fibonacci(base.toJava, max)

  def crontab(cronExpr: CronExpr): Policy = Policy(Fix(Crontab(cronExpr)))

  def jitter(min: Duration, max: Duration): Policy             = Policy(Fix(Jitter(min, max)))
  def jitter(min: FiniteDuration, max: FiniteDuration): Policy = jitter(min.toJava, max.toJava)

  def delays(head: Duration, tail: Duration*): Policy = Policy(Fix(Delays(NonEmptyList(head, tail.toList))))
  def delays(head: FiniteDuration, tail: FiniteDuration*): Policy =
    Policy(Fix(Delays(NonEmptyList(head.toJava, tail.toList.map(_.toJava)))))

  val giveUp: Policy = Policy(Fix(GiveUp()))
}
