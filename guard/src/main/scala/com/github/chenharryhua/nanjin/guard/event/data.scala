package com.github.chenharryhua.nanjin.guard.event

import cats.implicits.toFunctorOps
import com.github.chenharryhua.nanjin.common.chrono.Tick
import io.circe.generic.JsonCodec
import io.circe.syntax.EncoderOps
import io.circe.{Decoder, DecodingFailure, Encoder, Json}
import org.apache.commons.lang3.exception.ExceptionUtils

import java.time.ZonedDateTime
import scala.jdk.CollectionConverters.ListHasAsScala

@JsonCodec
final case class Error private (message: String, stack: List[String])

object Error {
  def apply(ex: Throwable): Error =
    Error(
      ExceptionUtils.getRootCauseMessage(ex),
      ExceptionUtils.getRootCauseStackTraceList(ex).asScala.map(_.replace("\t", "")).toList)
}

@JsonCodec
sealed trait MetricIndex extends Product {
  def launchTime: ZonedDateTime
}

object MetricIndex {
  final case class Adhoc(value: ZonedDateTime) extends MetricIndex {
    override val launchTime: ZonedDateTime = value
  }
  final case class Periodic(tick: Tick) extends MetricIndex {
    override val launchTime: ZonedDateTime = tick.zoned(_.conclude)
  }
}

sealed abstract class ServiceStopCause(val exitCode: Int) extends Product

object ServiceStopCause {
  case object Successfully extends ServiceStopCause(0)
  case object Maintenance extends ServiceStopCause(1)
  case object ByCancellation extends ServiceStopCause(2)
  final case class ByException(error: Error) extends ServiceStopCause(3)

  private val SUCCESSFULLY: String = "Successfully"
  private val BY_CANCELLATION: String = "ByCancellation"
  private val MAINTENANCE: String = "Maintenance"
  private val BY_EXCEPTION: String = "ByException"

  implicit val encoderServiceStopCause: Encoder[ServiceStopCause] = Encoder.instance {
    case Successfully       => Json.fromString(SUCCESSFULLY)
    case ByCancellation     => Json.fromString(BY_CANCELLATION)
    case ByException(error) => Json.obj(BY_EXCEPTION -> error.asJson)
    case Maintenance        => Json.fromString(MAINTENANCE)
  }

  implicit val decoderServiceStopCause: Decoder[ServiceStopCause] =
    List[Decoder[ServiceStopCause]](
      _.as[String].flatMap {
        case SUCCESSFULLY    => Right(Successfully)
        case BY_CANCELLATION => Right(ByCancellation)
        case MAINTENANCE     => Right(Maintenance)
        case unknown         => Left(DecodingFailure(s"unrecognized: $unknown", Nil))
      }.widen,
      _.downField(BY_EXCEPTION).as[Error].map(err => ByException(err)).widen
    ).reduceLeft(_ or _)
}
