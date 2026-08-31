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
  final case class Expire[K](policy: K, ttl: Duration) extends PolicyF[K]

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
  val EXPIRE: String = "expire"

}

// don't extend AnyVal as monocle doesn't like it
// use case class for free equal method
final case class Policy private (private[chrono] val policy: Fix[PolicyF]) {
  import PolicyF.{Except, Expire, FollowedBy, Jitter, Limited, Meet, Offset, Repeat}
  override def toString: String = ShowPolicy(policy)

  /** Limit the policy to at most `num` ticks. Non-positive values produce an empty policy.
    */
  def limited(num: Int): Policy =
    Policy(Fix(Limited(policy, num)))

  /** Append another policy after this one is exhausted.
    *
    * Once this policy produces no more ticks, the follower takes over.
    */
  def followedBy(other: Policy): Policy = Policy(Fix(FollowedBy(policy, other.policy)))
  def followedBy(f: Policy.type => Policy): Policy = followedBy(f(Policy))

  /** Repeat this policy indefinitely. When the policy is exhausted, it restarts from the beginning.
    */
  def repeat: Policy = Policy(Fix(Repeat(policy)))

  /** Combine with another policy, taking the shorter snooze at each step.
    *
    * Terminates when either policy is exhausted.
    */
  def meet(other: Policy): Policy = Policy(Fix(Meet(policy, other.policy)))
  def meet(f: Policy.type => Policy): Policy = meet(f(Policy))

  /** Skip the tick whose conclude time matches the given local time, stretching the snooze to reach the next
    * tick instead.
    */
  def except(localTime: LocalTime): Policy = Policy(Fix(Except(policy, localTime)))
  def except(f: localTimes.type => LocalTime): Policy = except(f(localTimes))

  /** Add a fixed non-negative duration to each tick's snooze.
    */
  def offset(fd: FiniteDuration): Policy = {
    require(fd >= ScalaDuration.Zero, show"$fd must be non-negative")
    Policy(Fix(Offset(policy, fd.toJava)))
  }

  /** Add a random duration between `min` and `max` to each tick's snooze.
    *
    * @param min
    *   non-negative
    * @param max
    *   strictly bigger than min
    */
  def jitter(min: FiniteDuration, max: FiniteDuration): Policy = {
    require(min >= ScalaDuration.Zero, show"$min must be non-negative")
    require(max > min, show"$max must be strictly bigger than $min")
    Policy(Fix(Jitter(policy, min.toJava, max.toJava)))
  }

  /** Add a random duration between zero and `max` to each tick's snooze.
    *
    * @param max
    *   strictly bigger than zero
    */
  def jitter(max: FiniteDuration): Policy =
    jitter(ScalaDuration.Zero, max)

  /** Set an absolute time-to-live for this policy. After `ttl` has elapsed since the policy's launch time, no
    * more ticks are produced — regardless of `repeat`, `followedBy`, or any other combinator.
    *
    * @param ttl
    *   must be positive
    */
  def expire(ttl: FiniteDuration): Policy = {
    require(ttl > ScalaDuration.Zero, show"$ttl must be positive")
    Policy(Fix(Expire(policy, ttl.toJava)))
  }
}

object Policy {
  import PolicyF.{Crontab, Empty, FixedDelay, FixedRate}

  given Show[Policy] = Show.fromToString
  given Encoder[Policy] = (a: Policy) => CodecPolicy.encoder(a.policy)
  given Decoder[Policy] = (c: HCursor) => CodecPolicy.decoder(c).map(Policy(_))
  given Eq[Policy] = Eq.fromUniversalEquals[Policy]

  /** Schedule based on a cron expression. Produces a single tick at the next matching time. Use `.repeat` for
    * continuous scheduling.
    */
  def crontab(cronExpr: CronExpr): Policy = Policy(Fix(Crontab(cronExpr)))
  def crontab(f: crontabs.type => CronExpr): Policy = crontab(f(crontabs))

  /** Fixed-delay scheduling. Produces one tick per delay in the list, then exhausts. Use `.repeat` to cycle
    * through the delays indefinitely.
    *
    * All delays must be non-negative, and at least one must be strictly positive.
    */
  def fixedDelay(nel: NonEmptyList[FiniteDuration]): Policy = {
    require(nel.forall(_ >= ScalaDuration.Zero), "every delay must be non-negative")
    require(nel.exists(_ > ScalaDuration.Zero), "at least one delay must be positive")
    Policy(Fix(FixedDelay(nel.map(_.toJava))))
  }

  /** Varargs convenience for `fixedDelay`. */
  def fixedDelay(head: FiniteDuration, tail: FiniteDuration*): Policy =
    fixedDelay(NonEmptyList.of(head, tail*))

  /** Fixed-rate scheduling. Produces a single tick that maintains a constant period from the previous
    * conclude time. Use `.repeat` for continuous fixed-rate scheduling.
    *
    * @param delay
    *   must be positive
    */
  def fixedRate(delay: FiniteDuration): Policy = {
    require(delay > ScalaDuration.Zero, show"delay must be positive, but was $delay")
    Policy(Fix(FixedRate(delay.toJava)))
  }

  /** Adapter that lets an already-built `Policy` be supplied where a builder function
    * `f: Policy.type => Policy` is expected.
    *
    * Most APIs take the builder shape so callers can write `_.fixedDelay(1.second).repeat`, where the
    * argument is this `Policy` companion. When you instead hold a `Policy` value prepared elsewhere, there is
    * no companion to build from, so pass it through `fresh`:
    *
    * {{{
    *   val prepared: Policy = Policy.fixedDelay(1.second).repeat
    *   agent.circuitBreaker(3, _.fresh(prepared))
    * }}}
    *
    * The body is intentionally the identity function: `fresh` exists only to occupy the
    * `Policy.type => Policy` slot, returning the supplied policy unchanged.
    */
  def fresh(policy: Policy): Policy = policy

  val empty: Policy = Policy(Fix(Empty()))
}
