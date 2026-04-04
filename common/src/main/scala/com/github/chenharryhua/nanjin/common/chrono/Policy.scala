package com.github.chenharryhua.nanjin.common.chrono

import cats.data.NonEmptyList
import cats.derived.derived
import cats.kernel.Eq
import cats.syntax.show.showInterpolator
import cats.{Functor, Show}
import cron4s.CronExpr
import higherkindness.droste.data.Fix
import io.circe.{Decoder, Encoder, HCursor}

import java.time.{Duration, LocalTime}
import scala.concurrent.duration.{Duration as ScalaDuration, FiniteDuration}
import scala.jdk.DurationConverters.ScalaDurationOps

sealed trait PolicyF[K] extends Product derives Functor

private object PolicyF {

  final case class Empty[K]() extends PolicyF[K]
  final case class Crontab[K](cronExpr: CronExpr) extends PolicyF[K]
  final case class FixedDelay[K](delays: NonEmptyList[Duration]) extends PolicyF[K]
  final case class FixedRate[K](delay: Duration) extends PolicyF[K]

  final case class Limited[K](policy: K, limit: Int) extends PolicyF[K]
  final case class FollowedBy[K](leader: K, follower: K) extends PolicyF[K]
  final case class Repeat[K](policy: K) extends PolicyF[K]
  final case class Meet[K](first: K, second: K) extends PolicyF[K]
  final case class Except[K](policy: K, except: LocalTime) extends PolicyF[K]
  final case class Offset[K](policy: K, offset: Duration) extends PolicyF[K]
  final case class Jitter[K](policy: K, min: Duration, max: Duration) extends PolicyF[K]

  val EMPTY: String = "empty"
  val CRONTAB: String = "crontab"
  val JITTER: String = "jitter"
  val JITTER_MIN: String = "min"
  val JITTER_MAX: String = "max"
  val FIXED_DELAY: String = "fixedDelay"
  val FIXED_RATE: String = "fixedRate"
  val LIMITED: String = "limited"
  val POLICY: String = "policy"
  val FOLLOWED_BY: String = "followedBy"
  val FOLLOWED_BY_LEADER: String = "leader"
  val FOLLOWED_BY_FOLLOWER: String = "follower"
  val MEET: String = "meet"
  val MEET_FIRST: String = "first"
  val MEET_SECOND: String = "second"
  val REPEAT: String = "repeat"
  val EXCEPT: String = "except"
  val OFFSET: String = "offset"

}

// don't extend AnyVal as monocle doesn't like it
// use case class for free equal method
final case class Policy private (private[chrono] val policy: Fix[PolicyF]) {
  import PolicyF.{Except, FollowedBy, Jitter, Limited, Meet, Offset, Repeat}
  override def toString: String = ShowPolicy(policy)

  /** @param num
    *   non-positive num essentially disable the policy
    */
  def limited(num: Int): Policy =
    Policy(Fix(Limited(policy, num)))

  def followedBy(other: Policy): Policy = Policy(Fix(FollowedBy(policy, other.policy)))
  def followedBy(f: Policy.type => Policy): Policy = followedBy(f(Policy))

  def repeat: Policy = Policy(Fix(Repeat(policy)))

  def meet(other: Policy): Policy = Policy(Fix(Meet(policy, other.policy)))
  def meet(f: Policy.type => Policy): Policy = meet(f(Policy))

  def except(localTime: LocalTime): Policy = Policy(Fix(Except(policy, localTime)))
  def except(f: localTimes.type => LocalTime): Policy = except(f(localTimes))

  def offset(fd: FiniteDuration): Policy = {
    require(fd >= ScalaDuration.Zero, show"$fd should be non-negative")
    Policy(Fix(Offset(policy, fd.toJava)))
  }

  /** @param min
    *   non-negative
    * @param max
    *   bigger than zero
    *
    * max should be bigger than min
    */
  def jitter(min: FiniteDuration, max: FiniteDuration): Policy = {
    require(min >= ScalaDuration.Zero, show"$min should not be negative")
    require(max > min, show"$max should be strickly bigger than $min")
    Policy(Fix(Jitter(policy, min.toJava, max.toJava)))
  }

  /** @param max
    *   bigger than zero
    */
  def jitter(max: FiniteDuration): Policy =
    jitter(ScalaDuration.Zero, max)
}

object Policy {
  import PolicyF.{Crontab, Empty, FixedDelay, FixedRate}

  given Show[Policy] = Show.fromToString
  given Encoder[Policy] = (a: Policy) => CodecPolicy.encoder(a.policy)
  given Decoder[Policy] = (c: HCursor) => CodecPolicy.decoder(c).map(Policy(_))
  given Eq[Policy] = Eq.fromUniversalEquals[Policy]

  def crontab(cronExpr: CronExpr): Policy = Policy(Fix(Crontab(cronExpr)))
  def crontab(f: crontabs.type => CronExpr): Policy = crontab(f(crontabs))

  /** should be non-negative
    */
  def fixedDelay(head: FiniteDuration, tail: FiniteDuration*): Policy = {
    val nel: NonEmptyList[FiniteDuration] = NonEmptyList.of(head, tail*)
    require(nel.forall(_ >= ScalaDuration.Zero), "every delay should be positive or zero")
    require(nel.exists(_ > ScalaDuration.Zero), "at least one is positive")
    Policy(Fix(FixedDelay(nel.map(_.toJava))))
  }

  /** @param delay
    *   should be bigger than zero
    */
  def fixedRate(delay: FiniteDuration): Policy = {
    require(delay > ScalaDuration.Zero, show"$delay should be bigger than zero")
    Policy(Fix(FixedRate(delay.toJava)))
  }

  /** Returns the given Policy instance unchanged.
    */
  def fresh(policy: Policy): Policy = policy

  val empty: Policy = Policy(Fix(Empty()))
}
