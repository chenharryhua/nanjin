package com.github.chenharryhua.nanjin.common.chrono

import cats.data.NonEmptyList
import cats.syntax.all.*
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
  final case class Crontab[K](cronExpr: CronExpr) extends PolicyF[K]
  final case class Jitter[K](min: Duration, max: Duration) extends PolicyF[K]
  final case class FixedDelay[K](delays: NonEmptyList[Duration]) extends PolicyF[K]
  final case class FixedRate[K](delays: NonEmptyList[Duration]) extends PolicyF[K]

  final case class Limited[K](policy: K, limit: Int) extends PolicyF[K]
  final case class FollowedBy[K](first: K, second: K) extends PolicyF[K]
  final case class Repeat[K](policy: Fix[PolicyF]) extends PolicyF[K]
  final case class EndUp[K](policy: K, endUp: LocalTime) extends PolicyF[K]
  final case class Join[K](first: K, second: K) extends PolicyF[K]

  private type CalcTick = TickRequest => Option[Tick]
  final case class TickRequest(tick: Tick, now: Instant)

  private def algebra(zoneId: ZoneId): Algebra[PolicyF, LazyList[CalcTick]] =
    Algebra[PolicyF, LazyList[CalcTick]] {

      case GiveUp() => LazyList.empty

      case Accordance(policy) => policy

      case Crontab(cronExpr) =>
        val calcTick: CalcTick = { case TickRequest(tick, now) =>
          cronExpr.next(now.atZone(tick.zoneId)).map(zdt => tick.newTick(now, Duration.between(now, zdt)))
        }
        LazyList.continually(calcTick)

      case Jitter(min, max) =>
        val calcTick: CalcTick = { case TickRequest(tick, now) =>
          val delay = Duration.of(Random.between(min.toNanos, max.toNanos), ChronoUnit.NANOS)
          tick.newTick(now, delay).some
        }
        LazyList.continually(calcTick)

      case FixedDelay(delays) =>
        val seed: LazyList[CalcTick] = LazyList.from(delays.toList).map[CalcTick] { delay =>
          { case TickRequest(tick, now) => tick.newTick(now, delay).some }
        }
        LazyList.continually(seed).flatten

      case FixedRate(delays) =>
        val seed: LazyList[CalcTick] = LazyList.from(delays.toList).map[CalcTick] { delay =>
          { case TickRequest(tick, now) =>
            val multi = (Duration.between(tick.launchTime, now).toScala / delay.toScala).ceil.toLong
            val gap   = Duration.between(now, tick.launchTime.plus(delay.multipliedBy(multi)))
            if (gap === Duration.ZERO) {
              val gap = Duration.between(now, tick.launchTime.plus(delay.multipliedBy(multi + 1)))
              tick.newTick(now, gap).some
            } else tick.newTick(now, gap).some
          }
        }
        LazyList.continually(seed).flatten

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

      case Join(first, second) =>
        first.zip(second).map { case (fa: CalcTick, fb: CalcTick) =>
          (req: TickRequest) =>
            (fa(req), fb(req)).mapN { (ra, rb) =>
              if (ra.snooze > rb.snooze) ra else rb // longer win
            }
        }
    }

  private val fmt: DurationFormatter = DurationFormatter.defaultFormatter

  val showPolicy: Algebra[PolicyF, String] = Algebra[PolicyF, String] {
    case GiveUp()           => show"giveUp"
    case Accordance(policy) => show"accordance($policy)"
    case Crontab(cronExpr)  => show"crontab($cronExpr)"
    case Jitter(min, max)   => show"jitter(${fmt.format(min)}, ${fmt.format(max)})"

    case FixedDelay(delays) if delays.size === 1 => show"fixedDelay(${fmt.format(delays.head)})"
    case FixedDelay(delays)                      => show"fixedDelay(${fmt.format(delays.head)}, ...)"
    case FixedRate(delays) if delays.size === 1  => show"fixedRate(${fmt.format(delays.head)})"
    case FixedRate(delays)                       => show"fixedRate(${fmt.format(delays.head)}, ...)"

    // ops
    case Limited(policy, limit)    => show"$policy.limited($limit)"
    case FollowedBy(first, second) => show"$first.followedBy($second)"
    case Repeat(policy)            => show"${Policy(policy)}.repeat"
    case EndUp(policy, endUp)      => show"$policy.endUp($endUp)"
    case Join(first, second)       => show"$first.join($second)"
  }

  def decisions(policy: Fix[PolicyF], zoneId: ZoneId): LazyList[CalcTick] =
    scheme.cata(algebra(zoneId)).apply(policy)
}

final case class Policy(policy: Fix[PolicyF]) extends AnyVal {
  import PolicyF.{EndUp, FollowedBy, Join, Limited, Repeat}
  override def toString: String = scheme.cata(PolicyF.showPolicy).apply(policy)

  def limited(num: Int): Policy         = Policy(Fix(Limited(policy, num)))
  def followedBy(other: Policy): Policy = Policy(Fix(FollowedBy(policy, other.policy)))
  def repeat: Policy                    = Policy(Fix(Repeat(policy)))
  def join(other: Policy): Policy       = Policy(Fix(Join(policy, other.policy)))

  def endUp(localTime: LocalTime): Policy = Policy(Fix(EndUp(policy, localTime)))
  def endOfDay: Policy                    = endUp(LocalTime.MAX)
}

object Policy {
  implicit val showPolicy: Show[Policy] = _.toString
}

object policies {
  import PolicyF.{Accordance, Crontab, FixedDelay, FixedRate, GiveUp, Jitter}

  def accordance(policy: Policy): Policy = Policy(Fix(Accordance(policy.policy)))

  def crontab(cronExpr: CronExpr): Policy = Policy(Fix(Crontab(cronExpr)))

  def jitter(min: FiniteDuration, max: FiniteDuration): Policy = Policy(Fix(Jitter(min.toJava, max.toJava)))

  def fixedDelay(delays: NonEmptyList[Duration]): Policy = Policy(Fix(FixedDelay(delays)))
  def fixedDelay(head: FiniteDuration, tail: FiniteDuration*): Policy =
    fixedDelay(NonEmptyList(head.toJava, tail.toList.map(_.toJava)))

  def fixedRate(delays: NonEmptyList[Duration]): Policy = Policy(Fix(FixedRate(delays)))
  def fixedRate(head: FiniteDuration, tail: FiniteDuration*): Policy =
    fixedRate(NonEmptyList(head.toJava, tail.toList.map(_.toJava)))

  val giveUp: Policy = Policy(Fix(GiveUp()))
}
