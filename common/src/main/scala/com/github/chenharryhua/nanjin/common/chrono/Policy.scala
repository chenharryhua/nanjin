package com.github.chenharryhua.nanjin.common.chrono

import cats.data.NonEmptyList
import cats.syntax.all.*
import cats.{Functor, Show}
import com.github.chenharryhua.nanjin.common.DurationFormatter
import com.github.chenharryhua.nanjin.common.chrono.PolicyF.ExpireAt
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
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.ScalaDurationOps
import scala.util.{Random, Try}

sealed trait PolicyF[K] extends Product with Serializable

private object PolicyF extends all {

  implicit val functorPolicyF: Functor[PolicyF] = cats.derived.semiauto.functor[PolicyF]

  final case class GiveUp[K]() extends PolicyF[K]
  final case class Accordance[K](policy: K) extends PolicyF[K]
  final case class Crontab[K](cronExpr: CronExpr) extends PolicyF[K]
  final case class Jitter[K](min: Duration, max: Duration) extends PolicyF[K]
  final case class FixedDelay[K](delays: NonEmptyList[Duration]) extends PolicyF[K]
  final case class FixedRate[K](delay: Duration) extends PolicyF[K]

  final case class Limited[K](policy: K, limit: Int) extends PolicyF[K]
  final case class FollowedBy[K](leader: K, follower: K) extends PolicyF[K]
  final case class Repeat[K](policy: Fix[PolicyF]) extends PolicyF[K]
  final case class EndAt[K](policy: K, end: LocalTime) extends PolicyF[K]
  final case class ExpireAt[K](policy: K, expire: LocalDateTime) extends PolicyF[K]
  final case class Meet[K](first: K, second: K) extends PolicyF[K]

  type CalcTick = TickRequest => Option[Tick]
  final case class TickRequest(tick: Tick, now: Instant)

  @tailrec
  private def fixedRateSnooze(wakeup: Instant, now: Instant, delay: Duration, count: Long): Duration = {
    val next = wakeup.plus(delay.multipliedBy(count))
    if (next.isAfter(now)) Duration.between(now, next)
    else
      fixedRateSnooze(wakeup, now, delay, count + 1)
  }

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

      case FixedRate(delay) =>
        LazyList.continually { case TickRequest(tick, now) =>
          tick.newTick(now, fixedRateSnooze(tick.wakeup, now, delay, 1)).some
        }

      // ops
      case Limited(policy, limit) => policy.take(limit)

      case FollowedBy(leader, follower) => leader #::: follower

      case Repeat(policy) =>
        LazyList.continually(decisions(policy, zoneId)).flatten

      case EndAt(policy, end) =>
        val timeFrame: LazyList[Unit] = LazyList.unfold(ZonedDateTime.now(zoneId)) { prev =>
          val now     = ZonedDateTime.now(zoneId)
          val sameDay = now.toLocalDate === prev.toLocalDate
          val endTime = end.atDate(now.toLocalDate).atZone(zoneId)
          if (endTime.isAfter(now) && sameDay) Some(((), now)) else None
        }
        policy.zip(timeFrame).map(_._1)

      case ExpireAt(policy, expire) =>
        val timeFrame: LazyList[Unit] = LazyList.unfold(()) { _ =>
          if (expire.isAfter(LocalDateTime.now)) Some(((), ())) else None
        }
        policy.zip(timeFrame).map(_._1)

      // https://en.wikipedia.org/wiki/Join_and_meet
      case Meet(first, second) =>
        first.zip(second).map { case (fa: CalcTick, fb: CalcTick) =>
          (req: TickRequest) =>
            (fa(req), fb(req)).mapN { (ra, rb) =>
              if (ra.snooze < rb.snooze) ra else rb // shorter win
            }
        }
    }

  def decisions(policy: Fix[PolicyF], zoneId: ZoneId): LazyList[CalcTick] =
    scheme.cata(algebra(zoneId)).apply(policy)

  private val fmt: DurationFormatter = DurationFormatter.defaultFormatter

  private val GIVE_UP: String              = "giveUp"
  private val ACCORDANCE: String           = "accordance"
  private val CRONTAB: String              = "crontab"
  private val JITTER: String               = "jitter"
  private val JITTER_MIN: String           = "min"
  private val JITTER_MAX: String           = "max"
  private val FIXED_DELAY: String          = "fixedDelay"
  private val FIXED_RATE: String           = "fixedRate"
  private val LIMITED: String              = "limited"
  private val POLICY: String               = "policy"
  private val FOLLOWED_BY: String          = "followedBy"
  private val FOLLOWED_BY_LEADER: String   = "leader"
  private val FOLLOWED_BY_FOLLOWER: String = "follower"
  private val END_AT: String               = "endAt"
  private val EXPIRE_AT: String            = "expireAt"
  private val MEET: String                 = "meet"
  private val MEET_FIRST: String           = "first"
  private val MEET_SECOND: String          = "second"
  private val REPEAT: String               = "repeat"

  val showPolicy: Algebra[PolicyF, String] = Algebra[PolicyF, String] {
    case GiveUp()           => show"$GIVE_UP"
    case Accordance(policy) => show"$ACCORDANCE($policy)"
    case Crontab(cronExpr)  => show"$CRONTAB($cronExpr)"
    case Jitter(min, max)   => show"$JITTER(${fmt.format(min)}, ${fmt.format(max)})"

    case FixedDelay(delays) if delays.size === 1 => show"$FIXED_DELAY(${fmt.format(delays.head)})"
    case FixedDelay(delays)                      => show"$FIXED_DELAY(${fmt.format(delays.head)}, ...)"
    case FixedRate(delay)                        => show"$FIXED_RATE(${fmt.format(delay)})"

    // ops
    case Limited(policy, limit)       => show"$policy.$LIMITED($limit)"
    case FollowedBy(leader, follower) => show"$leader.$FOLLOWED_BY($follower)"
    case Repeat(policy)               => show"${Policy(policy)}.$REPEAT"
    case EndAt(policy, end)           => show"$policy.$END_AT($end)"
    case ExpireAt(policy, expire)     => show"$policy.$EXPIRE_AT($expire)"
    case Meet(first, second)          => show"$first.$MEET($second)"
  }

  // json encoder
  private val jsonAlgebra: Algebra[PolicyF, Json] = Algebra[PolicyF, Json] {
    case GiveUp() =>
      Json.obj(GIVE_UP -> Json.Null)
    case Accordance(policy) =>
      Json.obj(ACCORDANCE -> policy)
    case Crontab(cronExpr) =>
      Json.obj(CRONTAB -> cronExpr.asJson)
    case Jitter(min, max) =>
      Json.obj(JITTER -> Json.obj(JITTER_MIN -> min.asJson, JITTER_MAX -> max.asJson))
    case FixedDelay(delays) =>
      Json.obj(FIXED_DELAY -> delays.asJson)
    case FixedRate(delays) =>
      Json.obj(FIXED_RATE -> delays.asJson)
    case Limited(policy, limit) =>
      Json.obj(LIMITED -> limit.asJson, POLICY -> policy)
    case FollowedBy(leader, follower) =>
      Json.obj(FOLLOWED_BY -> Json.obj(FOLLOWED_BY_LEADER -> leader, FOLLOWED_BY_FOLLOWER -> follower))
    case Repeat(policy) =>
      Json.obj(REPEAT -> encoderFixPolicyF(policy))
    case EndAt(policy, end) =>
      Json.obj(END_AT -> end.asJson, POLICY -> policy)
    case ExpireAt(policy, expire) =>
      Json.obj(EXPIRE_AT -> expire.asJson, POLICY -> policy)
    case Meet(first, second) =>
      Json.obj(MEET -> Json.obj(MEET_FIRST -> first, MEET_SECOND -> second))
  }

  val encoderFixPolicyF: Encoder[Fix[PolicyF]] =
    (a: Fix[PolicyF]) => scheme.cata(jsonAlgebra).apply(a)

  // json decoder
  private val jsonCoalgebra: Coalgebra[PolicyF, HCursor] = {
    def giveUp(hc: HCursor): Result[GiveUp[HCursor]] =
      hc.get[Json](GIVE_UP).map(_ => GiveUp[HCursor]())

    def accordance(hc: HCursor): Result[Accordance[HCursor]] =
      hc.downField(ACCORDANCE).as[HCursor].map(Accordance[HCursor])

    def crontab(hc: HCursor): Result[Crontab[HCursor]] =
      hc.get[CronExpr](CRONTAB).map(ce => Crontab[HCursor](ce))

    def jitter(hc: HCursor): Result[Jitter[HCursor]] = {
      val min = hc.downField(JITTER).downField(JITTER_MIN).as[Duration]
      val max = hc.downField(JITTER).downField(JITTER_MAX).as[Duration]
      (min, max).mapN(Jitter[HCursor])
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
      val leader   = hc.downField(FOLLOWED_BY).downField(FOLLOWED_BY_LEADER).as[HCursor]
      val follower = hc.downField(FOLLOWED_BY).downField(FOLLOWED_BY_FOLLOWER).as[HCursor]
      (leader, follower).mapN(FollowedBy[HCursor])
    }

    def endAt(hc: HCursor): Result[EndAt[HCursor]] = {
      val ea  = hc.get[LocalTime](END_AT)
      val plc = hc.downField(POLICY).as[HCursor]
      (plc, ea).mapN(EndAt[HCursor])
    }

    def expireAt(hc: HCursor): Result[ExpireAt[HCursor]] = {
      val ea  = hc.get[LocalDateTime](EXPIRE_AT)
      val plc = hc.downField(POLICY).as[HCursor]
      (plc, ea).mapN(ExpireAt[HCursor])
    }

    def meet(hc: HCursor): Result[Meet[HCursor]] = {
      val first  = hc.downField(MEET).downField(MEET_FIRST).as[HCursor]
      val second = hc.downField(MEET).downField(MEET_SECOND).as[HCursor]
      (first, second).mapN(Meet[HCursor])
    }

    def repeat(hc: HCursor): Result[Repeat[HCursor]] =
      hc.downField(REPEAT).as[HCursor].flatMap(c => decoderFixPolicyF(c).map(Repeat[HCursor]))

    Coalgebra[PolicyF, HCursor] { hc =>
      val result =
        giveUp(hc)
          .orElse(crontab(hc))
          .orElse(jitter(hc))
          .orElse(fixedDelay(hc))
          .orElse(fixedRate(hc))
          .orElse(limited(hc))
          .orElse(endAt(hc))
          .orElse(expireAt(hc))
          .orElse(followedBy(hc))
          .orElse(accordance(hc))
          .orElse(meet(hc))
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

final case class Policy(policy: Fix[PolicyF]) { // don't extends AnyVal, monocle doesn't like it
  import PolicyF.{EndAt, FollowedBy, Limited, Meet, Repeat}
  override def toString: String = scheme.cata(PolicyF.showPolicy).apply(policy)

  def limited(num: Int): Policy         = Policy(Fix(Limited(policy, num)))
  def followedBy(other: Policy): Policy = Policy(Fix(FollowedBy(policy, other.policy)))
  def repeat: Policy                    = Policy(Fix(Repeat(policy)))
  def meet(other: Policy): Policy       = Policy(Fix(Meet(policy, other.policy)))

  def expireAt(localDateTime: LocalDateTime): Policy = Policy(Fix(ExpireAt(policy, localDateTime)))

  def endAt(localTime: LocalTime): Policy            = Policy(Fix(EndAt(policy, localTime)))
  def endAt(f: localTimes.type => LocalTime): Policy = endAt(f(localTimes))

  def endOfDay: Policy = endAt(LocalTime.MAX)
}

object Policy {
  implicit val showPolicy: Show[Policy] = _.toString

  implicit val encoderPolicy: Encoder[Policy] =
    (a: Policy) => PolicyF.encoderFixPolicyF(a.policy)

  implicit val decoderPolicy: Decoder[Policy] =
    (c: HCursor) => PolicyF.decoderFixPolicyF(c).map(Policy(_))

}

object policies {
  import PolicyF.{Accordance, Crontab, FixedDelay, FixedRate, GiveUp, Jitter}

  def accordance(policy: Policy): Policy = Policy(Fix(Accordance(policy.policy)))

  def crontab(cronExpr: CronExpr): Policy           = Policy(Fix(Crontab(cronExpr)))
  def crontab(f: crontabs.type => CronExpr): Policy = crontab(f(crontabs))

  def jitter(min: FiniteDuration, max: FiniteDuration): Policy = Policy(Fix(Jitter(min.toJava, max.toJava)))

  def fixedDelay(delays: NonEmptyList[Duration]): Policy = Policy(Fix(FixedDelay(delays)))
  def fixedDelay(head: FiniteDuration, tail: FiniteDuration*): Policy =
    fixedDelay(NonEmptyList(head.toJava, tail.toList.map(_.toJava)))

  def fixedRate(delay: FiniteDuration): Policy = Policy(Fix(FixedRate(delay.toJava)))

  val giveUp: Policy = Policy(Fix(GiveUp()))
}
