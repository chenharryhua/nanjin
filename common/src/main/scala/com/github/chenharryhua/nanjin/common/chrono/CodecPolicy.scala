package com.github.chenharryhua.nanjin.common.chrono
import cats.data.NonEmptyList
import cats.syntax.apply.given
import cron4s.CronExpr
import higherkindness.droste.data.Fix
import higherkindness.droste.{scheme, Algebra, Coalgebra}
import io.circe.Decoder.Result
import io.circe.syntax.EncoderOps
import io.circe.{Decoder, DecodingFailure, Encoder, HCursor, Json}
import org.apache.commons.lang3.exception.ExceptionUtils

import java.time.{Duration, LocalTime}
import scala.util.control.NonFatal

private object CodecPolicy {

  import PolicyF.*

  /*
   * Json Encoder
   */
  private val encoderAlgebra: Algebra[PolicyF, Json] = Algebra[PolicyF, Json] {
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
    (a: Fix[PolicyF]) => scheme.cata(encoderAlgebra).apply(a)

  /*
   * Json Decoder
   */
  private def readField[A: Decoder](hc: HCursor, field: String): Result[A] =
    hc.get[A](field)

  private def readNestedField[A: Decoder](hc: HCursor, parent: String, child: String): Result[A] =
    hc.downField(parent).downField(child).as[A]

  private def readNestedCursor(hc: HCursor, field: String): Result[HCursor] =
    hc.downField(field).as[HCursor]

  private val decoderCoalgebra: Coalgebra[PolicyF, HCursor] = {
    val variantKeys: Set[String] = Set(
      EMPTY,
      CRONTAB,
      JITTER,
      FIXED_DELAY,
      FIXED_RATE,
      LIMITED,
      FOLLOWED_BY,
      MEET,
      EXCEPT,
      OFFSET,
      REPEAT
    )

    def empty(hc: HCursor): Result[Empty[HCursor]] =
      readField[Boolean](hc, EMPTY).flatMap {
        case true  => Right(Empty[HCursor]())
        case false => Left(DecodingFailure(s"$EMPTY must be true", hc.history))
      }

    def crontab(hc: HCursor): Result[Crontab[HCursor]] =
      readField[CronExpr](hc, CRONTAB).map(Crontab[HCursor])

    def jitter(hc: HCursor): Result[Jitter[HCursor]] = {
      val min = readNestedField[Duration](hc, JITTER, JITTER_MIN)
      val max = readNestedField[Duration](hc, JITTER, JITTER_MAX)
      val plc = readNestedField[HCursor](hc, JITTER, POLICY)

      (plc, min, max).mapN(Jitter[HCursor])
    }

    def fixedDelay(hc: HCursor): Result[FixedDelay[HCursor]] =
      readField[NonEmptyList[Duration]](hc, FIXED_DELAY).map(FixedDelay[HCursor])

    def fixedRate(hc: HCursor): Result[FixedRate[HCursor]] =
      readField[Duration](hc, FIXED_RATE).map(FixedRate[HCursor])

    def limited(hc: HCursor): Result[Limited[HCursor]] = {
      val lmt = readField[Int](hc, LIMITED)
      val plc = readNestedCursor(hc, POLICY)
      (plc, lmt).mapN(Limited[HCursor])
    }

    def followedBy(hc: HCursor): Result[FollowedBy[HCursor]] = {
      val leader = readNestedField[HCursor](hc, FOLLOWED_BY, FOLLOWED_BY_LEADER)
      val follower = readNestedField[HCursor](hc, FOLLOWED_BY, FOLLOWED_BY_FOLLOWER)
      (leader, follower).mapN(FollowedBy[HCursor])
    }

    def meet(hc: HCursor): Result[Meet[HCursor]] = {
      val first = readNestedField[HCursor](hc, MEET, MEET_FIRST)
      val second = readNestedField[HCursor](hc, MEET, MEET_SECOND)
      (first, second).mapN(Meet[HCursor])
    }

    def repeat(hc: HCursor): Result[Repeat[HCursor]] =
      readNestedCursor(hc, REPEAT).map(Repeat[HCursor])

    def except(hc: HCursor): Result[Except[HCursor]] = {
      val ept = readField[LocalTime](hc, EXCEPT)
      val plc = readNestedCursor(hc, POLICY)
      (plc, ept).mapN(Except[HCursor])
    }

    def offset(hc: HCursor): Result[Offset[HCursor]] = {
      val ost = readField[Duration](hc, OFFSET)
      val plc = readNestedCursor(hc, POLICY)
      (plc, ost).mapN(Offset[HCursor])
    }

    def decodeVariant(hc: HCursor): Result[PolicyF[HCursor]] = {
      val present = hc.keys.getOrElse(Iterable.empty).toSet.intersect(variantKeys)
      present.toList match {
        case key :: Nil =>
          key match {
            case EMPTY       => empty(hc)
            case CRONTAB     => crontab(hc)
            case JITTER      => jitter(hc)
            case FIXED_DELAY => fixedDelay(hc)
            case FIXED_RATE  => fixedRate(hc)
            case LIMITED     => limited(hc)
            case FOLLOWED_BY => followedBy(hc)
            case MEET        => meet(hc)
            case EXCEPT      => except(hc)
            case OFFSET      => offset(hc)
            case REPEAT      => repeat(hc)
            case other       =>
              Left(DecodingFailure(s"Unsupported policy variant key: $other", hc.history))
          }
        case Nil =>
          Left(
            DecodingFailure(
              s"No policy variant key found. Expected one of: ${variantKeys.toList.sorted.mkString(",")}",
              hc.history))
        case many =>
          Left(
            DecodingFailure(
              s"Ambiguous policy payload; found multiple variant keys: ${many.sorted.mkString(",")}",
              hc.history))
      }
    }

    Coalgebra[PolicyF, HCursor] { hc =>
      decodeVariant(hc).fold(
        err => throw err, // scalafix:ok
        identity
      )
    }
  }

  val decoder: Decoder[Fix[PolicyF]] =
    (hc: HCursor) =>
      try Right(scheme.ana(decoderCoalgebra).apply(hc))
      catch
        case ex: DecodingFailure => Left(ex)
        case NonFatal(ex)        => Left(DecodingFailure(ExceptionUtils.getMessage(ex), hc.history))

}
