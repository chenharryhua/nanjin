package com.github.chenharryhua.nanjin.common.chrono

import cats.data.NonEmptyList
import cats.syntax.all.*
import cats.{Functor, Show}
import cron4s.CronExpr
import cron4s.lib.javatime.javaTemporalInstance
import cron4s.syntax.all.*
import higherkindness.droste.data.Fix
import higherkindness.droste.{scheme, Algebra, Coalgebra}
import io.circe.*
import io.circe.Decoder.Result
import io.circe.DecodingFailure.Reason
import io.circe.syntax.EncoderOps
import org.apache.commons.lang3.exception.ExceptionUtils
import org.typelevel.cats.time.instances.all

import java.time.*
import java.time.temporal.ChronoUnit
import scala.annotation.tailrec
import scala.concurrent.duration.{Duration as ScalaDuration, FiniteDuration}
import scala.jdk.DurationConverters.ScalaDurationOps
import scala.util.{Random, Try}

sealed trait PolicyF[K] extends Product

private object PolicyF extends all {

  implicit val functorPolicyF: Functor[PolicyF] = cats.derived.semiauto.functor[PolicyF]

  final case class GiveUp[K]() extends PolicyF[K]
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

  type CalcTick = TickRequest => Tick
  final case class TickRequest(tick: Tick, now: Instant)

  @tailrec
  private def fixedRateSnooze(wakeup: Instant, now: Instant, delay: Duration, count: Long): Instant = {
    val next = wakeup.plus(delay.multipliedBy(count))
    if (next.isAfter(now)) next
    else
      fixedRateSnooze(wakeup, now, delay, count + 1)
  }

  private val algebra: Algebra[PolicyF, LazyList[CalcTick]] =
    Algebra[PolicyF, LazyList[CalcTick]] {

      case GiveUp() => LazyList.empty

      case Crontab(cronExpr) =>
        val calcTick: CalcTick = { case TickRequest(tick, now) =>
          cronExpr.next(now.atZone(tick.zoneId)) match {
            case Some(value) => tick.nextTick(now, value.toInstant)
            case None        => // should not happen but in case
              sys.error(show"$cronExpr return None at $now. idx=${tick.index}")
          }
        }
        LazyList.continually(calcTick)

      case FixedDelay(delays) =>
        val seed: LazyList[CalcTick] = LazyList.from(delays.toList).map[CalcTick] { delay =>
          { case TickRequest(tick, now) => tick.nextTick(now, now.plus(delay)) }
        }
        LazyList.continually(seed).flatten

      case FixedRate(delay) =>
        LazyList.continually { case TickRequest(tick, now) =>
          tick.nextTick(now, fixedRateSnooze(tick.conclude, now, delay, 1))
        }

      // ops
      case Limited(policy, limit) => policy.take(limit)

      case FollowedBy(leader, follower) => leader #::: follower

      case Repeat(policy) => LazyList.continually(policy).flatten

      // https://en.wikipedia.org/wiki/Join_and_meet
      case Meet(first, second) =>
        first.zip(second).map { case (fa: CalcTick, fb: CalcTick) =>
          (req: TickRequest) =>
            val ra = fa(req)
            val rb = fb(req)
            if (ra.snooze < rb.snooze) ra else rb // shorter win
        }

      case Except(policy, except) =>
        policy.map { (f: CalcTick) => (req: TickRequest) =>
          val tick = f(req)
          if (tick.local(_.conclude).toLocalTime === except) {
            val nt = f(TickRequest(tick, tick.conclude))
            tick.withSnoozeStretch(nt.snooze)
          } else tick
        }

      case Offset(policy, offset) =>
        policy.map { (f: CalcTick) => (req: TickRequest) =>
          f(req).withSnoozeStretch(offset)
        }

      case Jitter(policy, min, max) =>
        policy.map { (f: CalcTick) => (req: TickRequest) =>
          val delay = Duration.of(Random.between(min.toNanos, max.toNanos), ChronoUnit.NANOS)
          f(req).withSnoozeStretch(delay)
        }
    }

  def decisions(policy: Fix[PolicyF]): LazyList[CalcTick] =
    scheme.cata(algebra).apply(policy)

  private val GIVE_UP: String = "giveUp"
  private val CRONTAB: String = "crontab"
  private val JITTER: String = "jitter"
  private val JITTER_MIN: String = "min"
  private val JITTER_MAX: String = "max"
  private val FIXED_DELAY: String = "fixedDelay"
  private val FIXED_RATE: String = "fixedRate"
  private val LIMITED: String = "limited"
  private val POLICY: String = "policy"
  private val FOLLOWED_BY: String = "followedBy"
  private val FOLLOWED_BY_LEADER: String = "leader"
  private val FOLLOWED_BY_FOLLOWER: String = "follower"
  private val MEET: String = "meet"
  private val MEET_FIRST: String = "first"
  private val MEET_SECOND: String = "second"
  private val REPEAT: String = "repeat"
  private val EXCEPT: String = "except"
  private val OFFSET: String = "offset"

  val showPolicy: Algebra[PolicyF, String] = Algebra[PolicyF, String] {
    case GiveUp()          => show"$GIVE_UP"
    case Crontab(cronExpr) => show"$CRONTAB($cronExpr)"

    case FixedDelay(delays) => show"$FIXED_DELAY(${delays.toList.mkString(",")})"
    case FixedRate(delay)   => show"$FIXED_RATE($delay)"

    // ops
    case Limited(policy, limit)       => show"$policy.$LIMITED($limit)"
    case FollowedBy(leader, follower) => show"$leader.$FOLLOWED_BY($follower)"
    case Repeat(policy)               => show"$policy.$REPEAT"
    case Meet(first, second)          => show"$first.$MEET($second)"
    case Except(policy, except)       => show"$policy.$EXCEPT($except)"
    case Offset(policy, offset)       => show"$policy.$OFFSET($offset)"
    case Jitter(policy, min, max)     => show"$policy.$JITTER($min,$max)"
  }

  // json encoder
  private val jsonAlgebra: Algebra[PolicyF, Json] = Algebra[PolicyF, Json] {
    case GiveUp() =>
      Json.obj(GIVE_UP -> Json.True)
    case Crontab(cronExpr) =>
      Json.obj(CRONTAB -> cronExpr.asJson)
    case FixedDelay(delays) =>
      Json.obj(FIXED_DELAY -> delays.asJson)
    case FixedRate(delays) =>
      Json.obj(FIXED_RATE -> delays.asJson)
    case Limited(policy, limit) =>
      Json.obj(LIMITED -> limit.asJson, POLICY -> policy)
    case FollowedBy(leader, follower) =>
      Json.obj(FOLLOWED_BY -> Json.obj(FOLLOWED_BY_LEADER -> leader, FOLLOWED_BY_FOLLOWER -> follower))
    case Repeat(policy) =>
      Json.obj(REPEAT -> policy)
    case Meet(first, second) =>
      Json.obj(MEET -> Json.obj(MEET_FIRST -> first, MEET_SECOND -> second))
    case Except(policy, except) =>
      Json.obj(EXCEPT -> except.asJson, POLICY -> policy)
    case Offset(policy, offset) =>
      Json.obj(OFFSET -> offset.asJson, POLICY -> policy)
    case Jitter(policy, min, max) =>
      Json.obj(JITTER -> Json.obj(JITTER_MIN -> min.asJson, JITTER_MAX -> max.asJson, POLICY -> policy))
  }

  val encoderFixPolicyF: Encoder[Fix[PolicyF]] =
    (a: Fix[PolicyF]) => scheme.cata(jsonAlgebra).apply(a)

  // json decoder
  private val jsonCoalgebra: Coalgebra[PolicyF, HCursor] = {
    def giveUp(hc: HCursor): Result[GiveUp[HCursor]] =
      hc.get[Json](GIVE_UP).map(_ => GiveUp[HCursor]())

    def crontab(hc: HCursor): Result[Crontab[HCursor]] =
      hc.get[CronExpr](CRONTAB).map(ce => Crontab[HCursor](ce))

    def jitter(hc: HCursor): Result[Jitter[HCursor]] = {
      val min = hc.downField(JITTER).downField(JITTER_MIN).as[Duration]
      val max = hc.downField(JITTER).downField(JITTER_MAX).as[Duration]
      val plc = hc.downField(JITTER).downField(POLICY).as[HCursor]

      (plc, min, max).mapN(Jitter[HCursor])
    }

    def fixedDelay(hc: HCursor): Result[FixedDelay[HCursor]] =
      hc.get[NonEmptyList[Duration]](FIXED_DELAY).map(FixedDelay[HCursor])

    def fixedRate(hc: HCursor): Result[FixedRate[HCursor]] =
      hc.get[Duration](FIXED_RATE).map(FixedRate[HCursor])

    def limited(hc: HCursor): Result[Limited[HCursor]] = {
      val lmt = hc.get[Int](LIMITED)
      val plc = hc.downField(POLICY).as[HCursor]
      (plc, lmt).mapN(Limited[HCursor])
    }

    def followedBy(hc: HCursor): Result[FollowedBy[HCursor]] = {
      val leader = hc.downField(FOLLOWED_BY).downField(FOLLOWED_BY_LEADER).as[HCursor]
      val follower = hc.downField(FOLLOWED_BY).downField(FOLLOWED_BY_FOLLOWER).as[HCursor]
      (leader, follower).mapN(FollowedBy[HCursor])
    }

    def meet(hc: HCursor): Result[Meet[HCursor]] = {
      val first = hc.downField(MEET).downField(MEET_FIRST).as[HCursor]
      val second = hc.downField(MEET).downField(MEET_SECOND).as[HCursor]
      (first, second).mapN(Meet[HCursor])
    }

    def repeat(hc: HCursor): Result[Repeat[HCursor]] =
      hc.downField(REPEAT).as[HCursor].map(Repeat[HCursor])

    def except(hc: HCursor): Result[Except[HCursor]] = {
      val ept = hc.get[LocalTime](EXCEPT)
      val plc = hc.downField(POLICY).as[HCursor]
      (plc, ept).mapN(Except[HCursor])
    }

    def offset(hc: HCursor): Result[Offset[HCursor]] = {
      val ost = hc.get[Duration](OFFSET)
      val plc = hc.downField(POLICY).as[HCursor]
      (plc, ost).mapN(Offset[HCursor])
    }

    Coalgebra[PolicyF, HCursor] { hc =>
      val result =
        giveUp(hc)
          .orElse(crontab(hc))
          .orElse(jitter(hc))
          .orElse(fixedDelay(hc))
          .orElse(fixedRate(hc))
          .orElse(limited(hc))
          .orElse(followedBy(hc))
          .orElse(meet(hc))
          .orElse(except(hc))
          .orElse(offset(hc))
          .orElse(repeat(hc))

      result match {
        case Left(value)  => throw value
        case Right(value) => value
      }
    }
  }

  val decoderFixPolicyF: Decoder[Fix[PolicyF]] =
    (hc: HCursor) =>
      Try(scheme.ana(jsonCoalgebra).apply(hc)).toEither.leftMap { ex =>
        val reason = Reason.CustomReason(ExceptionUtils.getMessage(ex))
        DecodingFailure(reason, hc)
      }
}

// don't extend AnyVal as monocle doesn't like it
// use case class for free equal method
final case class Policy private (private[chrono] val policy: Fix[PolicyF]) {
  import PolicyF.{Except, FollowedBy, Jitter, Limited, Meet, Offset, Repeat}
  override def toString: String = scheme.cata(PolicyF.showPolicy).apply(policy)

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
  import PolicyF.{Crontab, FixedDelay, FixedRate, GiveUp}

  implicit val showPolicy: Show[Policy] = _.toString

  implicit val encoderPolicy: Encoder[Policy] =
    (a: Policy) => PolicyF.encoderFixPolicyF(a.policy)

  implicit val decoderPolicy: Decoder[Policy] =
    (c: HCursor) => PolicyF.decoderFixPolicyF(c).map(Policy(_))

  def crontab(cronExpr: CronExpr): Policy = Policy(Fix(Crontab(cronExpr)))
  def crontab(f: crontabs.type => CronExpr): Policy = crontab(f(crontabs))

  def fixedDelay(delays: NonEmptyList[Duration]): Policy = {
    require(delays.forall(!_.isNegative), "every delay should be positive or zero")
    Policy(Fix(FixedDelay(delays)))
  }

  /** should be non-negative
    */
  def fixedDelay(head: FiniteDuration, tail: FiniteDuration*): Policy =
    fixedDelay(NonEmptyList(head.toJava, tail.toList.map(_.toJava)))

  /** @param delay
    *   should be bigger than zero
    */
  def fixedRate(delay: FiniteDuration): Policy = {
    require(delay > ScalaDuration.Zero, show"$delay should be bigger than zero")
    Policy(Fix(FixedRate(delay.toJava)))
  }

  val giveUp: Policy = Policy(Fix(GiveUp()))
}
