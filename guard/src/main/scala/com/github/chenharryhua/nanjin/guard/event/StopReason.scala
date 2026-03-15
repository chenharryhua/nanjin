package com.github.chenharryhua.nanjin.guard.event

import cats.Show
import cats.syntax.functor.given
import cats.syntax.show.toShow
import io.circe.syntax.EncoderOps
import io.circe.{Decoder, DecodingFailure, Encoder, Json}

sealed abstract class StopReason(val exitCode: Int) extends Product

object StopReason {
  case object Successfully extends StopReason(0)
  case object Maintenance extends StopReason(1)
  case object ByCancellation extends StopReason(2)
  final case class ByException(stackTrace: StackTrace) extends StopReason(3)

  private val SUCCESSFULLY: String = "Successfully"
  private val BY_CANCELLATION: String = "ByCancellation"
  private val MAINTENANCE: String = "Maintenance"

  given Show[StopReason] = {
    case Successfully            => SUCCESSFULLY
    case Maintenance             => MAINTENANCE
    case ByCancellation          => BY_CANCELLATION
    case ByException(stackTrace) => stackTrace.show
  }

  given Encoder[StopReason] =
    Encoder.instance {
      case Successfully   => Json.fromString(SUCCESSFULLY)
      case ByCancellation => Json.fromString(BY_CANCELLATION)
      case Maintenance    => Json.fromString(MAINTENANCE)

      case ByException(stackTrace) => stackTrace.asJson
    }

  given Decoder[StopReason] =
    List[Decoder[StopReason]](
      _.as[String].flatMap {
        case SUCCESSFULLY    => Right(Successfully)
        case BY_CANCELLATION => Right(ByCancellation)
        case MAINTENANCE     => Right(Maintenance)
        case unknown         => Left(DecodingFailure(s"unrecognized: $unknown", Nil))
      }.widen,
      _.as[StackTrace].map(err => ByException(err)).widen
    ).reduceLeft(_ or _)
}
