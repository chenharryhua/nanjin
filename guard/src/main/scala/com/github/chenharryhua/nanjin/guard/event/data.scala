package com.github.chenharryhua.nanjin.guard.event

import cats.effect.Unique
import cats.syntax.functor.toFunctorOps
import cats.syntax.show.toShow
import cats.{Hash, Show}
import com.github.chenharryhua.nanjin.common.DurationFormatter
import io.circe.Decoder.Result
import io.circe.syntax.EncoderOps
import io.circe.{Codec, Decoder, DecodingFailure, Encoder, HCursor, Json}
import org.apache.commons.lang3.exception.ExceptionUtils
import org.typelevel.cats.time.instances.localtime.localtimeInstances

import java.time.temporal.ChronoUnit
import java.time.{Duration, ZonedDateTime}
import scala.jdk.CollectionConverters.ListHasAsScala

opaque type StackTrace = List[String]
object StackTrace:
  def apply(ex: Throwable): StackTrace =
    ExceptionUtils.getRootCauseStackTraceList(ex).asScala.map(_.replace("\t", "")).toList
  extension (st: StackTrace) def value: List[String] = st
  given Codec[StackTrace] =
    Codec.from(
      Decoder.decodeList[String],
      Encoder.encodeList[String]
    )
  given Show[StackTrace] = _.mkString("\n\t")
end StackTrace

sealed abstract class StopReason(val exitCode: Int) extends Product

object StopReason {
  case object Successfully extends StopReason(0)
  case object Maintenance extends StopReason(1)
  case object ByCancellation extends StopReason(2)
  final case class ByException(stackTrace: StackTrace) extends StopReason(3)

  private val SUCCESSFULLY: String = "Successfully"
  private val BY_CANCELLATION: String = "ByCancellation"
  private val MAINTENANCE: String = "Maintenance"

  implicit val showStopReason: Show[StopReason] = {
    case Successfully            => SUCCESSFULLY
    case Maintenance             => MAINTENANCE
    case ByCancellation          => BY_CANCELLATION
    case ByException(stackTrace) => stackTrace.show
  }

  implicit val encoderStopReason: Encoder[StopReason] =
    Encoder.instance {
      case Successfully   => Json.fromString(SUCCESSFULLY)
      case ByCancellation => Json.fromString(BY_CANCELLATION)
      case Maintenance    => Json.fromString(MAINTENANCE)

      case ByException(stackTrace) => stackTrace.asJson
    }

  implicit val decoderStopReason: Decoder[StopReason] =
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

final case class Correlation private (value: String) extends AnyVal {
  override def toString: String = value
}
object Correlation {
  def apply(token: Unique.Token): Correlation = {
    val id = Integer.toUnsignedLong(Hash[Unique.Token].hash(token))
    Correlation(f"$id%010d")
  }

  implicit val showCorrelation: Show[Correlation] = Show.fromToString
  implicit val codecCorrelation: Codec[Correlation] = new Codec[Correlation] {
    override def apply(c: HCursor): Result[Correlation] = c.as[String].map(Correlation(_))
    override def apply(a: Correlation): Json = Json.fromString(a.value)
  }
}

final case class Took(value: Duration) extends AnyVal
object Took {
  implicit val showTook: Show[Took] = ut => DurationFormatter.defaultFormatter.format(ut.value)
  implicit val encoderTook: Encoder[Took] = Encoder.encodeDuration.contramap(_.value)
  implicit val decoderTook: Decoder[Took] = Decoder.decodeDuration.map(Took(_))
}

final case class Active(value: Duration) extends AnyVal
object Active {
  implicit val showActive: Show[Active] = ut => DurationFormatter.defaultFormatter.format(ut.value)
  implicit val encoderActive: Encoder[Active] = Encoder.encodeDuration.contramap(_.value)
  implicit val decoderActive: Decoder[Active] = Decoder.decodeDuration.map(Active(_))
}

final case class Snooze(value: Duration) extends AnyVal
object Snooze {
  implicit val showSnooze: Show[Snooze] = ut => DurationFormatter.defaultFormatter.format(ut.value)
  implicit val encoderSnooze: Encoder[Snooze] = Encoder.encodeDuration.contramap(_.value)
  implicit val decoderSnooze: Decoder[Snooze] = Decoder.decodeDuration.map(Snooze(_))
}

final case class Timestamp(value: ZonedDateTime) extends AnyVal
object Timestamp {
  implicit val showTimestamp: Show[Timestamp] = _.value.toLocalTime.truncatedTo(ChronoUnit.SECONDS).show
  implicit val encoderTimestamp: Encoder[Timestamp] = Encoder.encodeZonedDateTime.contramap(_.value)
  implicit val decoderTimestamp: Decoder[Timestamp] = Decoder.decodeZonedDateTime.map(Timestamp(_))
}

final case class Message(value: Json) extends AnyVal
object Message {
  implicit val codecMessage: Codec[Message] = new Codec[Message] {
    override def apply(c: HCursor): Result[Message] = c.as[Json].map(Message(_))
    override def apply(a: Message): Json = a.value
  }
}

final case class Domain(value: String) extends AnyVal
object Domain {
  implicit val showDomain: Show[Domain] = _.value
  implicit val encoderDomain: Encoder[Domain] = Encoder.encodeString.contramap(_.value)
  implicit val decoderDomain: Decoder[Domain] = Decoder.decodeString.map(Domain(_))
}

final case class Label(value: String) extends AnyVal
object Label {
  implicit val showLabel: Show[Label] = _.value
  implicit val encoderLabel: Encoder[Label] = Encoder.encodeString.contramap(_.value)
  implicit val decoderLabel: Decoder[Label] = Decoder.decodeString.map(Label(_))
}
