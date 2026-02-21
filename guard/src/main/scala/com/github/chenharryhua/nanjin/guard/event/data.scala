package com.github.chenharryhua.nanjin.guard.event

import cats.effect.Unique
import cats.syntax.functor.toFunctorOps
import cats.syntax.show.toShow
import cats.{Hash, Show}
import com.github.chenharryhua.nanjin.common.DurationFormatter
import com.github.chenharryhua.nanjin.common.chrono.Tick
import io.circe.Decoder.Result
import io.circe.generic.JsonCodec
import io.circe.syntax.EncoderOps
import io.circe.{Codec, Decoder, DecodingFailure, Encoder, HCursor, Json}
import org.apache.commons.lang3.exception.ExceptionUtils
import org.typelevel.cats.time.instances.localtime.localtimeInstances

import java.time.temporal.ChronoUnit
import java.time.{Duration, ZonedDateTime}
import scala.jdk.CollectionConverters.ListHasAsScala

final case class StackTrace private (value: List[String]) extends AnyVal

object StackTrace {
  def apply(ex: Throwable): StackTrace =
    StackTrace(ExceptionUtils.getRootCauseStackTraceList(ex).asScala.map(_.replace("\t", "")).toList)

  implicit val showStackTrace: Show[StackTrace] = _.value.mkString("\n\t")
  implicit val codecStackTrace: Codec[StackTrace] = new Codec[StackTrace] {
    override def apply(c: HCursor): Result[StackTrace] = c.as[List[String]].map(StackTrace(_))
    override def apply(a: StackTrace): Json = a.value.asJson
  }
}

@JsonCodec
sealed trait Index extends Product {
  def launchTime: ZonedDateTime
}

object Index {
  final case class Adhoc(value: ZonedDateTime) extends Index {
    override val launchTime: ZonedDateTime = value
  }

  final case class Periodic(tick: Tick) extends Index {
    override val launchTime: ZonedDateTime = tick.zoned(_.conclude)
  }

  implicit val showIndex: Show[Index] = {
    case Adhoc(_)       => "Adhoc"
    case Periodic(tick) => tick.index.toString
  }
}

sealed abstract class ServiceStopCause(val exitCode: Int) extends Product

object ServiceStopCause {
  case object Successfully extends ServiceStopCause(0)
  case object Maintenance extends ServiceStopCause(1)
  case object ByCancellation extends ServiceStopCause(2)
  final case class ByException(stackTrace: StackTrace) extends ServiceStopCause(3)

  private val SUCCESSFULLY: String = "Successfully"
  private val BY_CANCELLATION: String = "ByCancellation"
  private val MAINTENANCE: String = "Maintenance"
  private val BY_EXCEPTION: String = "ByException"

  implicit val showServiceStopCause: Show[ServiceStopCause] = {
    case Successfully            => SUCCESSFULLY
    case Maintenance             => MAINTENANCE
    case ByCancellation          => BY_CANCELLATION
    case ByException(stackTrace) => stackTrace.show
  }

  implicit val encoderServiceStopCause: Encoder[ServiceStopCause] =
    Encoder.instance {
      case Successfully            => Json.fromString(SUCCESSFULLY)
      case ByCancellation          => Json.fromString(BY_CANCELLATION)
      case ByException(stackTrace) => Json.obj(BY_EXCEPTION -> stackTrace.asJson)
      case Maintenance             => Json.fromString(MAINTENANCE)
    }

  implicit val decoderServiceStopCause: Decoder[ServiceStopCause] =
    List[Decoder[ServiceStopCause]](
      _.as[String].flatMap {
        case SUCCESSFULLY    => Right(Successfully)
        case BY_CANCELLATION => Right(ByCancellation)
        case MAINTENANCE     => Right(Maintenance)
        case unknown         => Left(DecodingFailure(s"unrecognized: $unknown", Nil))
      }.widen,
      _.downField(BY_EXCEPTION).as[StackTrace].map(err => ByException(err)).widen
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
