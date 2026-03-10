package com.github.chenharryhua.nanjin.guard.config

import cats.Show
import cats.syntax.show.toShow
import com.github.chenharryhua.nanjin.guard.translator.durationFormatter
import enumeratum.values.{CatsOrderValueEnum, CatsValueEnum, IntCirceEnum, IntEnum, IntEnumEntry}
import enumeratum.{CatsEnum, CirceEnum, Enum, EnumEntry}
import io.circe.Decoder.Result
import io.circe.{Codec, Decoder, Encoder, HCursor, Json}
import org.slf4j.event.Level

import java.time.{Duration, ZoneId}
import java.util.UUID

sealed abstract class AlarmLevel(override val value: Int, val level: Level)
    extends IntEnumEntry with Product {
  val entryName: String = this.productPrefix.toLowerCase()
}

object AlarmLevel
    extends CatsOrderValueEnum[Int, AlarmLevel] with IntEnum[AlarmLevel] with IntCirceEnum[AlarmLevel]
    with CatsValueEnum[Int, AlarmLevel] {
  override val values: IndexedSeq[AlarmLevel] = findValues

  case object Error extends AlarmLevel(4, Level.ERROR)
  case object Warn extends AlarmLevel(3, Level.WARN)
  case object Good extends AlarmLevel(2, Level.INFO)
  case object Info extends AlarmLevel(1, Level.INFO)
  case object Debug extends AlarmLevel(0, Level.DEBUG)
}

sealed trait LogFormat extends EnumEntry
object LogFormat extends Enum[LogFormat] with CirceEnum[LogFormat] with CatsEnum[LogFormat] {
  override def values: IndexedSeq[LogFormat] = findValues

  case object Console_PlainText extends LogFormat
  case object Console_Json_OneLine extends LogFormat
  case object Console_Json_MultiLine extends LogFormat
  case object Console_JsonVerbose extends LogFormat

  case object Slf4j_PlainText extends LogFormat
  case object Slf4j_Json_OneLine extends LogFormat
  case object Slf4j_Json_MultiLine extends LogFormat
}

final case class Task(value: String) extends AnyVal
object Task {
  implicit val showTask: Show[Task] = _.value
  implicit val encoderTask: Encoder[Task] = Encoder.encodeString.contramap(_.value)
  implicit val decoderTask: Decoder[Task] = Decoder.decodeString.map(Task(_))
}

final case class Service(value: String) extends AnyVal
object Service {
  implicit val showService: Show[Service] = _.value
  implicit val encoderService: Encoder[Service] = Encoder.encodeString.contramap(_.value)
  implicit val decoderService: Decoder[Service] = Decoder.decodeString.map(Service(_))
}

final case class ServiceId(value: UUID) extends AnyVal
object ServiceId {
  implicit val showServiceId: Show[ServiceId] = _.value.show
  implicit val encoderServiceId: Encoder[ServiceId] = Encoder.encodeUUID.contramap(_.value)
  implicit val decoderServiceId: Decoder[ServiceId] = Decoder.decodeUUID.map(ServiceId(_))
}

final case class Homepage(value: String) extends AnyVal
object Homepage {
  implicit val encoderHomepage: Encoder[Homepage] = Encoder.encodeString.contramap(_.value)
  implicit val decoderHomepage: Decoder[Homepage] = Decoder.decodeString.map(Homepage(_))
}

final case class Port(value: Int) extends AnyVal
object Port {
  implicit val showPort: Show[Port] = _.value.toString
  implicit val encoderPort: Encoder[Port] = Encoder.encodeInt.contramap(_.value)
  implicit val decoderPort: Decoder[Port] = Decoder.decodeInt.map(Port(_))
}

final case class Brief(value: Json) extends AnyVal
object Brief {
  implicit val showServiceBrief: Show[Brief] = _.value.spaces2
  implicit val encoderServiceBrief: Encoder[Brief] = _.value
  implicit val decoderServiceBrief: Decoder[Brief] = Decoder.instance(_.as[Json]).map(Brief(_))
}

object data:
  opaque type TimeZone = ZoneId
  object TimeZone:
    def apply(zoneId: ZoneId): TimeZone = zoneId
    extension (tz: TimeZone) def value: ZoneId = tz
    given Show[TimeZone] = Show.fromToString
    given Codec[TimeZone] with
      override def apply(c: HCursor): Result[TimeZone] =
        Decoder.decodeZoneId(c)
      override def apply(a: TimeZone): Json =
        Encoder.encodeZoneId.apply(a.value)

  opaque type UpTime = Duration
  object UpTime:
    def apply(duration: Duration): UpTime = duration
    extension (upTime: UpTime) def value: Duration = upTime
    given Show[UpTime] = durationFormatter.format(_)
    given Codec[UpTime] with
      override def apply(c: HCursor): Result[UpTime] =
        Decoder.decodeDuration.apply(c)
      override def apply(a: UpTime): Json =
        Encoder.encodeDuration.apply(a.value)
