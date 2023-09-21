package com.github.chenharryhua.nanjin.common.chrono

import cats.implicits.{showInterpolator, toShow}
import cats.{Functor, Show}
import com.github.chenharryhua.nanjin.common.DurationFormatter
import cron4s.CronExpr
import cron4s.lib.javatime.javaTemporalInstance
import higherkindness.droste.data.Fix
import higherkindness.droste.{scheme, Algebra}
import org.typelevel.cats.time.instances.localtime

import java.time.*
import java.time.temporal.ChronoUnit
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.{JavaDurationOps, ScalaDurationOps}
import scala.util.Random
sealed trait Manipulation
private object Manipulation {
  case object ResetCounter extends Manipulation
  case object DoNothing extends Manipulation
}

sealed trait PolicyF[K]

private object PolicyF extends localtime {

  implicit val functorPolicyF: Functor[PolicyF] = cats.derived.semiauto.functor[PolicyF]

  final case class GiveUp[K]() extends PolicyF[K]
  final case class Constant[K](baseDelay: Duration) extends PolicyF[K]
  final case class FixedPace[K](baseDelay: Duration) extends PolicyF[K]
  final case class Exponential[K](baseDelay: Duration) extends PolicyF[K]
  final case class Fibonacci[K](baseDelay: Duration) extends PolicyF[K]
  final case class Crontab[K](cronExpr: CronExpr) extends PolicyF[K]
  final case class Jitter[K](min: Duration, max: Duration) extends PolicyF[K]

  final case class Limited[K](policy: K, limit: Int) extends PolicyF[K]
  final case class FollowedBy[K](first: K, second: K) extends PolicyF[K]
  final case class Capped[K](policy: K, cap: Duration) extends PolicyF[K]
  final case class Repeat[K](policy: K) extends PolicyF[K]
  final case class EndUp[K](policy: K, endUp: LocalTime) extends PolicyF[K]

  final case class TickRequest(tick: Tick, counter: Int, now: Instant)
  private type CalcTick = TickRequest => Either[Manipulation, Tick]

  private val resetCounter: CalcTick = _ => Left(Manipulation.ResetCounter)

  private def algebra(zoneId: ZoneId): Algebra[PolicyF, LazyList[CalcTick]] =
    Algebra[PolicyF, LazyList[CalcTick]] {

      case GiveUp() => LazyList.empty

      case Constant(baseDelay) =>
        LazyList.continually { case TickRequest(tick, _, now) => Right(tick.newTick(now, baseDelay)) }

      case FixedPace(baseDelay) =>
        val delay: CalcTick = { case TickRequest(tick, _, now) =>
          Right {
            val multi = (Duration.between(tick.launchTime, now).toScala / baseDelay.toScala).ceil.toLong
            val dur   = Duration.between(now, tick.launchTime.plus(baseDelay.multipliedBy(multi)))
            if (dur == Duration.ZERO) {
              val nd = Duration.between(now, tick.launchTime.plus(baseDelay.multipliedBy(multi + 1)))
              tick.newTick(now, nd)
            } else tick.newTick(now, dur)
          }
        }
        LazyList.continually(delay)

      case Exponential(baseDelay) =>
        val delay: CalcTick = { case TickRequest(tick, counter, now) =>
          Right(tick.newTick(now, baseDelay.multipliedBy(Math.pow(2, counter.toDouble).toLong)))
        }
        LazyList.continually(delay)

      case Fibonacci(baseDelay) =>
        val delay: CalcTick = { case TickRequest(tick, counter, now) =>
          Right(tick.newTick(now, baseDelay.multipliedBy(Fib.fibonacci(counter + 1))))
        }
        LazyList.continually(delay)

      case Crontab(cronExpr) =>
        val delay: CalcTick = { case TickRequest(tick, _, now) =>
          cronExpr.next(now.atZone(zoneId)).map(zdt => Duration.between(now, zdt)) match {
            case Some(value) => Right(tick.newTick(now, value))
            case None        => Left(Manipulation.DoNothing)
          }
        }
        LazyList.continually(delay)

      case Jitter(min, max) =>
        val delay: CalcTick = { case TickRequest(tick, _, now) =>
          Right(tick.newTick(now, Duration.of(Random.between(min.toNanos, max.toNanos), ChronoUnit.NANOS)))
        }
        LazyList.continually(delay)

      // ops
      case Limited(policy, limit) => policy.take(limit)

      case FollowedBy(first, second) => first #::: (resetCounter #:: second)

      case Capped(policy, cap) =>
        policy.map(_.andThen(_.map { tk =>
          if (tk.snooze.compareTo(cap) < 0) tk else tk.newTick(tk.acquire, cap)
        }))

      case Repeat(policy) => LazyList.continually(resetCounter #:: policy).flatten

      case EndUp(policy, endUp) =>
        val timeFrame: LazyList[Unit] = LazyList.unfold(ZonedDateTime.now(zoneId)) { prev =>
          val now     = ZonedDateTime.now(zoneId)
          val sameDay = now.toLocalDate == prev.toLocalDate
          val endTime = endUp.atDate(now.toLocalDate).atZone(zoneId)
          if (endTime.isAfter(now) && sameDay) Some(((), now)) else None
        }
        policy.zip(timeFrame).map(_._1)
    }

  private val fmt: DurationFormatter = DurationFormatter.defaultFormatter

  val show: Algebra[PolicyF, String] = Algebra[PolicyF, String] {
    case GiveUp()               => show"GiveUp"
    case Constant(baseDelay)    => show"Constant(${fmt.format(baseDelay)})"
    case FixedPace(baseDelay)   => show"FixRate(${fmt.format(baseDelay)})"
    case Exponential(baseDelay) => show"Exponential(${fmt.format(baseDelay)})"
    case Fibonacci(baseDelay)   => show"Fibonacci(${fmt.format(baseDelay)})"
    case Crontab(cronExpr)      => show"Cron(${cronExpr.show})"
    case Jitter(min, max)       => show"Jitter(${fmt.format(min)},${fmt.format(max)})"
    // ops
    case Limited(policy, limit)    => show"${policy.show}.limited($limit)"
    case FollowedBy(first, second) => show"${first.show}.followedBy(${second.show})"
    case Capped(policy, cap)       => show"${policy.show}.capped(${fmt.format(cap)})"
    case Repeat(policy)            => show"${policy.show}.repeat"
    case EndUp(policy, endUp)      => show"${policy.show}.endUp(${endUp.show})"
  }

  def decisions(policy: Fix[PolicyF], zoneId: ZoneId): LazyList[CalcTick] =
    scheme.cata(algebra(zoneId)).apply(policy)
}

final case class Policy(policy: Fix[PolicyF]) extends AnyVal {
  import PolicyF.{Capped, EndUp, FollowedBy, Limited, Repeat}
  override def toString: String = scheme.cata(PolicyF.show).apply(policy)

  def limited(num: Int): Policy         = Policy(Fix(Limited(policy, num)))
  def followedBy(other: Policy): Policy = Policy(Fix(FollowedBy(policy, other.policy)))
  def repeat: Policy                    = Policy(Fix(Repeat(policy)))

  def capped(cap: Duration): Policy       = Policy(Fix(Capped(policy, cap)))
  def capped(cap: FiniteDuration): Policy = capped(cap.toJava)

  def endUp(lt: LocalTime): Policy = Policy(Fix(EndUp(policy, lt)))
  def endOfDay: Policy             = endUp(LocalTime.MAX)
}

object Policy {
  implicit val showPolicy: Show[Policy] = _.toString
}

object policies {
  import PolicyF.{Constant, Crontab, Exponential, Fibonacci, FixedPace, GiveUp, Jitter}
  def constant(baseDelay: Duration): Policy       = Policy(Fix(Constant(baseDelay)))
  def constant(baseDelay: FiniteDuration): Policy = constant(baseDelay.toJava)

  def exponential(baseDelay: Duration): Policy       = Policy(Fix(Exponential(baseDelay)))
  def exponential(baseDelay: FiniteDuration): Policy = exponential(baseDelay.toJava)

  def fixedPace(baseDelay: Duration): Policy       = Policy(Fix(FixedPace(baseDelay)))
  def fixedPace(baseDelay: FiniteDuration): Policy = fixedPace(baseDelay.toJava)

  def fibonacci(baseDelay: Duration): Policy       = Policy(Fix(Fibonacci(baseDelay)))
  def fibonacci(baseDelay: FiniteDuration): Policy = fibonacci(baseDelay.toJava)

  def crontab(cronExpr: CronExpr): Policy = Policy(Fix(Crontab(cronExpr)))

  def jitter(min: Duration, max: Duration): Policy             = Policy(Fix(Jitter(min, max)))
  def jitter(min: FiniteDuration, max: FiniteDuration): Policy = jitter(min.toJava, max.toJava)

  val giveUp: Policy = Policy(Fix(GiveUp()))

}
