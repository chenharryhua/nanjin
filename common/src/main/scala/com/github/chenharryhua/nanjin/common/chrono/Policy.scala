package com.github.chenharryhua.nanjin.common.chrono

import cats.implicits.{showInterpolator, toShow}
import cats.{Functor, Show}
import com.github.chenharryhua.nanjin.common.DurationFormatter
import cron4s.CronExpr
import cron4s.lib.javatime.javaTemporalInstance
import higherkindness.droste.data.Fix
import higherkindness.droste.{scheme, Algebra}

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

final private case class TickRequest(tick: Tick, counter: Int, now: Instant)

sealed trait PolicyF[K]

private object PolicyF {
  private type ComputeDelay = TickRequest => Either[Manipulation, Duration]
  implicit val functorPolicyF: Functor[PolicyF] = cats.derived.semiauto.functor[PolicyF]

  final case class GiveUp[K]() extends PolicyF[K]
  final case class Constant[K](baseDelay: Duration) extends PolicyF[K]
  final case class FixedPace[K](baseDelay: Duration) extends PolicyF[K]
  final case class Exponential[K](baseDelay: Duration) extends PolicyF[K]
  final case class Fibonacci[K](baseDelay: Duration) extends PolicyF[K]
  final case class Crontab[K](cronExpr: CronExpr, zoneId: ZoneId) extends PolicyF[K]
  final case class Jitter[K](min: Duration, max: Duration) extends PolicyF[K]

  final case class Limited[K](policy: K, limit: Int) extends PolicyF[K]
  final case class FollowedBy[K](first: K, second: K) extends PolicyF[K]
  final case class Capped[K](policy: K, cap: Duration) extends PolicyF[K]
  final case class Repeat[K](policy: K) extends PolicyF[K]

  private val resetCounter: ComputeDelay = _ => Left(Manipulation.ResetCounter)

  private val algebra: Algebra[PolicyF, LazyList[ComputeDelay]] = Algebra[PolicyF, LazyList[ComputeDelay]] {
    case GiveUp()            => LazyList.empty
    case Constant(baseDelay) => LazyList.continually(_ => Right(baseDelay))
    case FixedPace(baseDelay) =>
      val delay: ComputeDelay = { case TickRequest(tick, _, now) =>
        Right {
          val multi = (Duration.between(tick.launchTime, now).toScala / baseDelay.toScala).ceil.toLong
          Duration.between(now, tick.launchTime.plus(baseDelay.multipliedBy(multi)))
        }
      }
      LazyList.continually(delay)
    case Exponential(baseDelay) =>
      val delay: ComputeDelay = { case TickRequest(_, counter, _) =>
        Right(baseDelay.multipliedBy(Math.pow(2, counter.toDouble).toLong))
      }
      LazyList.continually(delay)
    case Fibonacci(baseDelay) =>
      val delay: ComputeDelay = { case TickRequest(_, counter, _) =>
        Right(baseDelay.multipliedBy(Fib.fibonacci(counter + 1)))
      }
      LazyList.continually(delay)
    case Crontab(cronExpr, zoneId) =>
      val delay: ComputeDelay = { case TickRequest(_, _, now) =>
        cronExpr.next(now.atZone(zoneId)).map(zdt => Duration.between(now, zdt)) match {
          case Some(value) => Right(value)
          case None        => Left(Manipulation.DoNothing)
        }
      }
      LazyList.continually(delay)
    case Jitter(min, max) =>
      val delay: ComputeDelay =
        _ => Right(Duration.of(Random.between(min.toNanos, max.toNanos), ChronoUnit.NANOS))
      LazyList.continually(delay)

    case Limited(policy, limit)    => policy.take(limit)
    case FollowedBy(first, second) => first #::: (resetCounter #:: second)
    case Capped(policy, cap)       => policy.map(_.andThen(_.map(d => if (d.compareTo(cap) < 0) d else cap)))
    case Repeat(policy)            => LazyList.continually(resetCounter #:: policy).flatten
  }

  private val fmt: DurationFormatter = DurationFormatter.defaultFormatter

  val show: Algebra[PolicyF, String] = Algebra[PolicyF, String] {
    case GiveUp()                  => show"GiveUp"
    case Constant(baseDelay)       => show"Constant(${fmt.format(baseDelay)})"
    case FixedPace(baseDelay)      => show"FixRate(${fmt.format(baseDelay)})"
    case Exponential(baseDelay)    => show"Exponential(${fmt.format(baseDelay)})"
    case Fibonacci(baseDelay)      => show"Fibonacci(${fmt.format(baseDelay)})"
    case Crontab(cronExpr, _)      => show"Cron(${cronExpr.show})"
    case Jitter(min, max)          => show"Jitter(${fmt.format(min)},${fmt.format(max)})"
    case Limited(policy, limit)    => show"(${policy.show}+$limit)"
    case FollowedBy(first, second) => show"${first.show}.followBy(${second.show})"
    case Capped(policy, cap)       => show"${policy.show}.capped(${fmt.format(cap)})"
    case Repeat(policy)            => show"${policy.show}.repeat"
  }

  def decisions(policy: Fix[PolicyF]): LazyList[ComputeDelay] =
    scheme.cata(algebra).apply(policy)
}

final case class Policy(policy: Fix[PolicyF]) extends AnyVal {
  import PolicyF.{Capped, FollowedBy, Limited, Repeat}
  override def toString: String = scheme.cata(PolicyF.show).apply(policy)

  def limited(num: Int): Policy           = Policy(Fix(Limited(policy, num)))
  def followedBy(other: Policy): Policy   = Policy(Fix(FollowedBy(policy, other.policy)))
  def repeat: Policy                      = Policy(Fix(Repeat(policy)))
  def capped(cap: Duration): Policy       = Policy(Fix(Capped(policy, cap)))
  def capped(cap: FiniteDuration): Policy = capped(cap.toJava)
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

  def crontab(cronExpr: CronExpr, zoneId: ZoneId): Policy = Policy(Fix(Crontab(cronExpr, zoneId)))

  def jitter(min: Duration, max: Duration): Policy             = Policy(Fix(Jitter(min, max)))
  def jitter(min: FiniteDuration, max: FiniteDuration): Policy = jitter(min.toJava, max.toJava)

  val giveUp: Policy = Policy(Fix(GiveUp()))

}
