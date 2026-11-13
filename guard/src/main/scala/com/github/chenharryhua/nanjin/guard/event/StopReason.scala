package com.github.chenharryhua.nanjin.guard.event

import cats.Show
import com.github.chenharryhua.nanjin.guard.config.StackTrace
import io.circe.syntax.EncoderOps
import io.circe.{Decoder, DecodingFailure, Encoder, Json}

enum StopReason(val exitCode: Int):
  case Successfully extends StopReason(0)
  case Maintenance extends StopReason(1)
  case ByCancellation extends StopReason(2)
  case ByException(stackTrace: StackTrace) extends StopReason(3)

object StopReason {
  private val SUCCESSFULLY: String = "Successfully"
  private val BY_CANCELLATION: String = "ByCancellation"
  private val MAINTENANCE: String = "Maintenance"
  private val BY_EXCEPTION: String = "ByException"

  given Show[StopReason] = {
    case Successfully   => SUCCESSFULLY
    case Maintenance    => MAINTENANCE
    case ByCancellation => BY_CANCELLATION
    case ByException(_) => BY_EXCEPTION
  }

  /** Writes an '''untagged''' form: the three plain cases become their own name as a JSON string, while
    * `ByException` becomes the bare stack trace, a JSON array of frames.
    *
    * Two consequences for a consumer, both deliberate and both load-bearing for existing payloads:
    *   - there is no discriminator field, so the failure case is recognised by the value being an array
    *     rather than a string (`BY_EXCEPTION` names the case for `Show`, and is never written to JSON);
    *   - `exitCode` is not serialized. Recover it from the decoded variant, which carries it.
    */
  given Encoder[StopReason] =
    Encoder.instance {
      case Successfully   => Json.fromString(SUCCESSFULLY)
      case ByCancellation => Json.fromString(BY_CANCELLATION)
      case Maintenance    => Json.fromString(MAINTENANCE)

      case ByException(stackTrace) => stackTrace.asJson
    }

  /** Reads the untagged form described on `Encoder`, dispatching on the shape of the JSON rather than trying
    * each variant in turn.
    *
    * The accepted payloads are exactly those the encoder writes: one of the three names as a string, or a
    * stack trace as an array. Shape dispatch is what makes the failure message useful — chaining the two
    * decoders with `or` reported the '''last''' one's error, so `"Garbage"` complained about a `List[String]`
    * type mismatch instead of naming the unrecognized string.
    */
  given Decoder[StopReason] =
    Decoder.instance { c =>
      c.value.asString match {
        case Some(SUCCESSFULLY)    => Right(Successfully)
        case Some(BY_CANCELLATION) => Right(ByCancellation)
        case Some(MAINTENANCE)     => Right(Maintenance)
        case Some(unknown)         => Left(DecodingFailure(s"unrecognized: $unknown", c.history))
        case None                  => c.as[StackTrace].map(ByException(_))
      }
    }
}
