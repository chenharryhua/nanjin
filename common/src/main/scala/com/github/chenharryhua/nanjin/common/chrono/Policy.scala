package com.github.chenharryhua.nanjin.common.chrono

import cats.data.NonEmptyList
import cats.implicits.{catsSyntaxEq, showInterpolator}
import cats.{Functor, Show}
import com.github.chenharryhua.nanjin.common.DurationFormatter
import com.github.chenharryhua.nanjin.common.chrono.PolicyF.Threshold
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

sealed trait Manipulation
private object Manipulation {
  case object ResetCounter extends Manipulation
  case object DoNothing extends Manipulation
}

sealed trait PolicyF[K]

private object PolicyF extends localtime with localdate with duration {

  implicit val functorPolicyF: Functor[PolicyF] = cats.derived.semiauto.functor[PolicyF]

  final case class GiveUp[K]() extends PolicyF[K]
  final case class Accordance[K](policy: K) extends PolicyF[K]
  final case class Constant[K](baseDelay: Duration) extends PolicyF[K]
  final case class FixedPace[K](baseDelay: Duration) extends PolicyF[K]
  final case class Exponential[K](baseDelay: Duration) extends PolicyF[K]
  final case class Fibonacci[K](baseDelay: Duration) extends PolicyF[K]
  final case class Crontab[K](cronExpr: CronExpr) extends PolicyF[K]
  final case class Jitter[K](min: Duration, max: Duration) extends PolicyF[K]
  final case class Delays[K](delays: NonEmptyList[Duration]) extends PolicyF[K]

  final case class Limited[K](policy: K, limit: Int) extends PolicyF[K]
  final case class FollowedBy[K](first: K, second: K) extends PolicyF[K]
  final case class Capped[K](policy: K, cap: Duration) extends PolicyF[K]
  final case class Repeat[K](policy: K) extends PolicyF[K]
  final case class Threshold[K](policy: K, threshold: Duration) extends PolicyF[K]

  final case class TickRequest(tick: Tick, counter: Int, now: Instant)
  private type CalcTick = TickRequest => Either[Manipulation, Tick]

  private val resetCounter: CalcTick = _ => Left(Manipulation.ResetCounter)

  private def algebra(zoneId: ZoneId): Algebra[PolicyF, LazyList[CalcTick]] =
    Algebra[PolicyF, LazyList[CalcTick]] {

      case GiveUp() => LazyList.empty

      case Accordance(policy) => policy

      case Constant(baseDelay) =>
        LazyList.continually { case TickRequest(tick, _, now) => Right(tick.newTick(now, baseDelay)) }

      case FixedPace(baseDelay) =>
        val calcTick: CalcTick = { case TickRequest(tick, _, now) =>
          Right {
            val multi = (Duration.between(tick.launchTime, now).toScala / baseDelay.toScala).ceil.toLong
            val gap   = Duration.between(now, tick.launchTime.plus(baseDelay.multipliedBy(multi)))
            if (gap === Duration.ZERO) {
              val gap = Duration.between(now, tick.launchTime.plus(baseDelay.multipliedBy(multi + 1)))
              tick.newTick(now, gap)
            } else tick.newTick(now, gap)
          }
        }
        LazyList.continually(calcTick)

      case Exponential(baseDelay) =>
        val calcTick: CalcTick = { case TickRequest(tick, counter, now) =>
          Right(tick.newTick(now, baseDelay.multipliedBy(Math.pow(2, counter.toDouble).toLong)))
        }
        LazyList.continually(calcTick)

      case Fibonacci(baseDelay) =>
        val calcTick: CalcTick = { case TickRequest(tick, counter, now) =>
          Right(tick.newTick(now, baseDelay.multipliedBy(Fib.fibonacci(counter + 1))))
        }
        LazyList.continually(calcTick)

      case Crontab(cronExpr) =>
        val calcTick: CalcTick = { case TickRequest(tick, _, now) =>
          cronExpr.next(now.atZone(zoneId)).map(zdt => Duration.between(now, zdt)) match {
            case Some(value) => Right(tick.newTick(now, value))
            case None        => Left(Manipulation.DoNothing)
          }
        }
        LazyList.continually(calcTick)

      case Jitter(min, max) =>
        val calcTick: CalcTick = { case TickRequest(tick, _, now) =>
          Right(tick.newTick(now, Duration.of(Random.between(min.toNanos, max.toNanos), ChronoUnit.NANOS)))
        }
        LazyList.continually(calcTick)

      case Delays(delays) =>
        LazyList.from(delays.toList.map { delay =>
          { case TickRequest(tick, _, now) => Right(tick.newTick(now, delay)) }
        })

      // ops
      case Limited(policy, limit) => policy.take(limit)

      case FollowedBy(first, second) => first #::: (resetCounter #:: second)

      case Capped(policy, cap) =>
        policy.map { calcTick => (req: TickRequest) =>
          calcTick(req).map { tick =>
            val gap = tick.snooze.compareTo(cap)
            if (gap <= 0) tick else req.tick.newTick(req.now, cap) // preserve index
          }
        }

      case Threshold(policy, threshold) =>
        policy.map { calcTick => (req: TickRequest) =>
          calcTick(req).flatMap { tick =>
            val gap = Duration.between(req.now, tick.wakeup)
            if (gap.compareTo(threshold) < 0) Right(tick) else Left(Manipulation.ResetCounter)
          }
        }

      case Repeat(policy) => LazyList.continually(resetCounter #:: policy).flatten
    }

  private val fmt: DurationFormatter = DurationFormatter.defaultFormatter

  val show: Algebra[PolicyF, String] = Algebra[PolicyF, String] {
    case GiveUp()               => show"giveUp"
    case Accordance(policy)     => show"accordance($policy)"
    case Constant(baseDelay)    => show"constant(${fmt.format(baseDelay)})"
    case FixedPace(baseDelay)   => show"fixedPace(${fmt.format(baseDelay)})"
    case Exponential(baseDelay) => show"exponential(${fmt.format(baseDelay)})"
    case Fibonacci(baseDelay)   => show"fibonacci(${fmt.format(baseDelay)})"
    case Crontab(cronExpr)      => show"crontab($cronExpr)"
    case Jitter(min, max)       => show"jitter(${fmt.format(min)}, ${fmt.format(max)})"
    case Delays(delays)         => show"delays(${fmt.format(delays.head)}, ...)"
    // ops
    case Limited(policy, limit)       => show"$policy.limited($limit)"
    case FollowedBy(first, second)    => show"$first.followedBy($second)"
    case Capped(policy, cap)          => show"$policy.capped(${fmt.format(cap)})"
    case Threshold(policy, threshold) => show"$policy.threshold(${fmt.format(threshold)})"
    case Repeat(policy)               => show"$policy.repeat"
  }

  def decisions(policy: Fix[PolicyF], zoneId: ZoneId): LazyList[CalcTick] =
    scheme.cata(algebra(zoneId)).apply(policy)
}

final case class Policy(policy: Fix[PolicyF]) extends AnyVal {
  import PolicyF.{Capped, FollowedBy, Limited, Repeat}
  override def toString: String = scheme.cata(PolicyF.show).apply(policy)

  def limited(num: Int): Policy         = Policy(Fix(Limited(policy, num)))
  def followedBy(other: Policy): Policy = Policy(Fix(FollowedBy(policy, other.policy)))
  def repeat: Policy                    = Policy(Fix(Repeat(policy)))

  def capped(duration: Duration): Policy       = Policy(Fix(Capped(policy, duration)))
  def capped(duration: FiniteDuration): Policy = capped(duration.toJava)

  def threshold(duration: Duration): Policy       = Policy(Fix(Threshold(policy, duration)))
  def threshold(duration: FiniteDuration): Policy = threshold(duration.toJava)

}

object Policy {
  implicit val showPolicy: Show[Policy] = _.toString
}

object policies {
  import PolicyF.{Accordance, Constant, Crontab, Delays, Exponential, Fibonacci, FixedPace, GiveUp, Jitter}

  def accordance(policy: Policy): Policy = Policy(Fix(Accordance(policy.policy)))

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

  def delays(head: Duration, tail: Duration*): Policy = Policy(Fix(Delays(NonEmptyList(head, tail.toList))))
  def delays(head: FiniteDuration, tail: FiniteDuration*): Policy =
    Policy(Fix(Delays(NonEmptyList(head.toJava, tail.toList.map(_.toJava)))))

  val giveUp: Policy = Policy(Fix(GiveUp()))
}
