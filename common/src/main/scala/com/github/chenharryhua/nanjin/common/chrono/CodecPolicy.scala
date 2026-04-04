package com.github.chenharryhua.nanjin.common.chrono
import cats.data.NonEmptyList
import cats.syntax.apply.given
import cats.syntax.either.given
import cron4s.CronExpr
import higherkindness.droste.data.Fix
import higherkindness.droste.{scheme, Algebra, Coalgebra}
import io.circe.Decoder.Result
import io.circe.DecodingFailure.Reason
import io.circe.syntax.EncoderOps
import io.circe.{Decoder, DecodingFailure, Encoder, HCursor, Json}
import org.apache.commons.lang3.exception.ExceptionUtils

import java.time.{Duration, LocalTime}
import scala.util.Try

private object CodecPolicy {

  import PolicyF.*

  /*
   * Json Encoder
   */
  private val jsonAlgebra: Algebra[PolicyF, Json] = Algebra[PolicyF, Json] {
    case Empty() =>
      Json.obj(EMPTY -> Json.True)
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

  val encoder: Encoder[Fix[PolicyF]] =
    (a: Fix[PolicyF]) => scheme.cata(jsonAlgebra).apply(a)

  /*
   * Json Decoder
   */
  private val jsonCoalgebra: Coalgebra[PolicyF, HCursor] = {
    def empty(hc: HCursor): Result[Empty[HCursor]] =
      hc.get[Json](EMPTY).map(_ => Empty[HCursor]())

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
        empty(hc)
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
        case Left(value)  => throw value // scalafix:ok
        case Right(value) => value
      }
    }
  }

  val decoder: Decoder[Fix[PolicyF]] =
    (hc: HCursor) =>
      Try(scheme.ana(jsonCoalgebra).apply(hc)).toEither.leftMap { ex =>
        val reason = Reason.CustomReason(ExceptionUtils.getMessage(ex))
        DecodingFailure(reason, hc)
      }

}
