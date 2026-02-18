package com.github.chenharryhua.nanjin.guard.event

import cats.effect.Unique
import cats.syntax.functor.toFunctorOps
import cats.syntax.show.toShow
import cats.{Hash, Show}
import com.github.chenharryhua.nanjin.common.DurationFormatter
import io.circe.generic.JsonCodec
import io.circe.syntax.EncoderOps
import io.circe.{Decoder, DecodingFailure, Encoder, Json}
import org.apache.commons.lang3.exception.ExceptionUtils
import org.typelevel.cats.time.instances.localtime.localtimeInstances

import java.time.temporal.ChronoUnit
import java.time.{Duration, ZonedDateTime}
import scala.jdk.CollectionConverters.ListHasAsScala

@JsonCodec
final case class Error private (message: String, stack: List[String])

object Error {
  def apply(ex: Throwable): Error =
    Error(
      ExceptionUtils.getRootCauseMessage(ex),
      ExceptionUtils.getRootCauseStackTraceList(ex).asScala.map(_.replace("\t", "")).toList)

  implicit val showError: Show[Error] = _.stack.mkString("\n\t")
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

  implicit val showServiceStopCause: Show[ServiceStopCause] = {
    case Successfully       => SUCCESSFULLY
    case Maintenance        => MAINTENANCE
    case ByCancellation     => BY_CANCELLATION
    case ByException(error) => error.show
  }

  implicit val encoderServiceStopCause: Encoder[ServiceStopCause] =
    Encoder.instance {
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

final case class Correlation private (value: String) extends AnyVal {
  override def toString: String = value
}
object Correlation {
  def apply(token: Unique.Token): Correlation = {
    val id = Integer.toUnsignedLong(Hash[Unique.Token].hash(token))
    Correlation(f"$id%010d")
  }

  implicit val showCorrelation: Show[Correlation] = Show.fromToString
  implicit val encoderCorrelation: Encoder[Correlation] = Encoder.encodeString.contramap(_.value)
  implicit val decoderCorrelation: Decoder[Correlation] = Decoder.decodeString.map(Correlation(_))
}

final case class Took(value: Duration) extends AnyVal
object Took {
  implicit val showTook: Show[Took] = ut => DurationFormatter.defaultFormatter.format(ut.value)
  implicit val encoderTook: Encoder[Took] = Encoder.encodeDuration.contramap(_.value)
  implicit val decoderTook: Decoder[Took] = Decoder.decodeDuration.map(Took(_))
}

final case class Timestamp(value: ZonedDateTime) extends AnyVal
object Timestamp {
  implicit val showTimestamp: Show[Timestamp] = _.value.toLocalTime.truncatedTo(ChronoUnit.SECONDS).show
  implicit val encoderTimestamp: Encoder[Timestamp] = Encoder.encodeZonedDateTime.contramap(_.value)
  implicit val decoderTimestamp: Decoder[Timestamp] = Decoder.decodeZonedDateTime.map(Timestamp(_))
}

final case class Index(value: Long) extends AnyVal
object Index {
  implicit val showIndex: Show[Index] = _.value.toString
  implicit val encoderIndex: Encoder[Index] = Encoder.encodeLong.contramap(_.value)
  implicit val decoderIndex: Decoder[Index] = Decoder.decodeLong.map(Index(_))
}
