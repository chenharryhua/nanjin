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

/** JSON codec for the policy AST.
  *
  * The decoder enforces the same invariants the `Policy` builder does. The encoder only ever writes policies
  * that came through that builder, so any payload failing these checks was not produced here, and accepting
  * one would build a policy whose steppers misbehave when stepped rather than failing at the decode boundary.
  * A `fixed_rate` of zero, for instance, has no next cadence boundary to compute.
  */
private object CodecPolicy {

  import PolicyF.*

  /*
   * Json Encoder
   */
  private val encoder_algebra: Algebra[PolicyF, Json] = Algebra[PolicyF, Json] {
    case Empty() =>
      Json.obj(EMPTY -> Json.True)
    case Crontab(cronExpr) =>
      Json.obj(CRONTAB -> cronExpr.asJson)
    case FixedDelay(delays) =>
      Json.obj(FIXED_DELAY -> delays.asJson)
    case FixedRate(delay) =>
      Json.obj(FIXED_RATE -> delay.asJson)
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
    (a: Fix[PolicyF]) => scheme.cata(encoder_algebra).apply(a)

  /*
   * Json Decoder
   */
  private def read_field[A: Decoder](hc: HCursor, field: String): Result[A] =
    hc.get[A](field)

  private def read_nested_field[A: Decoder](hc: HCursor, parent: String, child: String): Result[A] =
    hc.downField(parent).downField(child).as[A]

  private def read_nested_cursor(hc: HCursor, field: String): Result[HCursor] =
    hc.downField(field).as[HCursor]

  private val decoder_coalgebra: Coalgebra[PolicyF, HCursor] = {

    def empty(hc: HCursor): Result[Empty[HCursor]] =
      read_field[Boolean](hc, EMPTY).flatMap {
        case true  => Right(Empty[HCursor]())
        case false => Left(DecodingFailure(s"$EMPTY must be true", hc.history))
      }

    def crontab(hc: HCursor): Result[Crontab[HCursor]] =
      read_field[CronExpr](hc, CRONTAB).map(Crontab[HCursor])

    def is_positive(d: Duration): Boolean = !d.isNegative && !d.isZero

    def reject[A](hc: HCursor, message: String): Result[A] =
      Left(DecodingFailure(message, hc.history))

    def jitter(hc: HCursor): Result[Jitter[HCursor]] = {
      val min = read_nested_field[Duration](hc, JITTER, JITTER_MIN)
      val max = read_nested_field[Duration](hc, JITTER, JITTER_MAX)
      val plc = read_nested_field[HCursor](hc, JITTER, POLICY)

      (plc, min, max).mapN(Jitter[HCursor]).flatMap { decoded =>
        if (decoded.min.isNegative)
          reject(hc, s"$JITTER_MIN must be non-negative, but was ${decoded.min}")
        else if (decoded.max.compareTo(decoded.min) <= 0)
          reject(
            hc,
            s"$JITTER_MAX must be strictly bigger than $JITTER_MIN ${decoded.min}, but was ${decoded.max}")
        else Right(decoded)
      }
    }

    def fixed_delay(hc: HCursor): Result[FixedDelay[HCursor]] =
      read_field[NonEmptyList[Duration]](hc, FIXED_DELAY).flatMap { delays =>
        if (delays.exists(_.isNegative))
          reject(hc, s"every $FIXED_DELAY must be non-negative, but got ${delays.toList.mkString(",")}")
        else if (!delays.exists(is_positive))
          reject(hc, s"at least one $FIXED_DELAY must be positive, but got ${delays.toList.mkString(",")}")
        else Right(FixedDelay[HCursor](delays))
      }

    def fixed_rate(hc: HCursor): Result[FixedRate[HCursor]] =
      read_field[Duration](hc, FIXED_RATE).flatMap { delay =>
        if (is_positive(delay)) Right(FixedRate[HCursor](delay))
        else reject(hc, s"$FIXED_RATE must be positive, but was $delay")
      }

    def limited(hc: HCursor): Result[Limited[HCursor]] = {
      val lmt = read_field[Int](hc, LIMITED)
      val plc = read_nested_cursor(hc, POLICY)
      (plc, lmt).mapN(Limited[HCursor])
    }

    def followed_by(hc: HCursor): Result[FollowedBy[HCursor]] = {
      val leader = read_nested_field[HCursor](hc, FOLLOWED_BY, FOLLOWED_BY_LEADER)
      val follower = read_nested_field[HCursor](hc, FOLLOWED_BY, FOLLOWED_BY_FOLLOWER)
      (leader, follower).mapN(FollowedBy[HCursor])
    }

    def meet(hc: HCursor): Result[Meet[HCursor]] = {
      val first = read_nested_field[HCursor](hc, MEET, MEET_FIRST)
      val second = read_nested_field[HCursor](hc, MEET, MEET_SECOND)
      (first, second).mapN(Meet[HCursor])
    }

    def repeat(hc: HCursor): Result[Repeat[HCursor]] =
      read_nested_cursor(hc, REPEAT).map(Repeat[HCursor])

    def except(hc: HCursor): Result[Except[HCursor]] = {
      val ept = read_field[LocalTime](hc, EXCEPT)
      val plc = read_nested_cursor(hc, POLICY)
      (plc, ept).mapN(Except[HCursor])
    }

    def offset(hc: HCursor): Result[Offset[HCursor]] = {
      val ost = read_field[Duration](hc, OFFSET)
      val plc = read_nested_cursor(hc, POLICY)
      (plc, ost).mapN(Offset[HCursor]).flatMap { decoded =>
        if (decoded.offset.isNegative)
          reject(hc, s"$OFFSET must be non-negative, but was ${decoded.offset}")
        else Right(decoded)
      }
    }

    /** The key a payload is recognised by, paired with the decoder for that variant.
      *
      * One map rather than a key set beside a dispatch match: the set of recognised keys and the decoder
      * reached for each are the same object, so adding a variant to one cannot leave the other behind, and
      * there is no unreachable "unsupported key" arm to carry.
      */
    val variant_decoders: Map[String, HCursor => Result[PolicyF[HCursor]]] = Map(
      EMPTY -> empty,
      CRONTAB -> crontab,
      JITTER -> jitter,
      FIXED_DELAY -> fixed_delay,
      FIXED_RATE -> fixed_rate,
      LIMITED -> limited,
      FOLLOWED_BY -> followed_by,
      MEET -> meet,
      EXCEPT -> except,
      OFFSET -> offset,
      REPEAT -> repeat
    )

    def decode_variant(hc: HCursor): Result[PolicyF[HCursor]] = {
      val present = hc.keys.getOrElse(Iterable.empty).toSet.intersect(variant_decoders.keySet)
      present.toList match {
        // `present` is an intersection with the key set, so every key in it has a decoder
        case key :: Nil => variant_decoders(key)(hc)
        case Nil        =>
          val expected = variant_decoders.keys.toList.sorted.mkString(",")
          Left(DecodingFailure(s"No policy variant key found. Expected one of: $expected", hc.history))
        case many =>
          Left(
            DecodingFailure(
              s"Ambiguous policy payload; found multiple variant keys: ${many.sorted.mkString(",")}",
              hc.history))
      }
    }

    // droste's `Coalgebra[PolicyF, HCursor]` must be a pure `HCursor => PolicyF[HCursor]`; the unfold driven
    // by `scheme.ana` has no Either/monadic channel to surface a decode failure. We therefore throw the
    // DecodingFailure here and rely on `decoder` (below) to run this coalgebra inside a try/catch that turns
    // it back into a `Left`. The exception never escapes `decoder`, so the resulting `Decoder` stays total.
    //
    // This coalgebra is `private`, so `decoder` below is its only unfold site today. The invariant to keep:
    // any future `scheme.ana(decoder_coalgebra)` added in this object must run inside the same catch (or use
    // `scheme.anaM` over the Either monad), otherwise the throw would leak past a total Decoder boundary.
    Coalgebra[PolicyF, HCursor] { hc =>
      decode_variant(hc).fold(
        // caught by `decoder`'s try/catch; see note above
        err => throw err, // scalafix:ok
        identity
      )
    }
  }

  val decoder: Decoder[Fix[PolicyF]] =
    (hc: HCursor) =>
      // This try/catch is the other half of decoder_coalgebra's invariant: it confines the DecodingFailure
      // that the coalgebra throws mid-unfold and converts it back to a `Left`, keeping this Decoder total.
      try Right(scheme.ana(decoder_coalgebra).apply(hc))
      catch
        case ex: DecodingFailure => Left(ex)
        case NonFatal(ex)        => Left(DecodingFailure(ExceptionUtils.getMessage(ex), hc.history))

}
