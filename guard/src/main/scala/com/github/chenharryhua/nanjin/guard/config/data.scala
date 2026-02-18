package com.github.chenharryhua.nanjin.guard.config

import cats.Show
import cats.syntax.show.toShow
import com.github.chenharryhua.nanjin.guard.translator.durationFormatter
import enumeratum.values.{CatsOrderValueEnum, CatsValueEnum, IntCirceEnum, IntEnum, IntEnumEntry}
import enumeratum.{CirceEnum, Enum, EnumEntry}
import io.circe.{Decoder, Encoder, Json}

import java.time.{Duration, ZoneId}
import java.util.UUID

sealed abstract class AlarmLevel(override val value: Int, val entryName: String)
    extends IntEnumEntry with Product

object AlarmLevel
    extends CatsOrderValueEnum[Int, AlarmLevel] with IntEnum[AlarmLevel] with IntCirceEnum[AlarmLevel]
    with CatsValueEnum[Int, AlarmLevel] {
  override val values: IndexedSeq[AlarmLevel] = findValues

  case object Error extends AlarmLevel(4, "error")
  case object Warn extends AlarmLevel(3, "warn")
  case object Done extends AlarmLevel(2, "done")
  case object Info extends AlarmLevel(1, "info")
  case object Debug extends AlarmLevel(0, "debug")
}

sealed trait LogFormat extends EnumEntry
object LogFormat extends Enum[LogFormat] with CirceEnum[LogFormat] {
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

final case class Domain(value: String) extends AnyVal
object Domain {
  implicit val showDomain: Show[Domain] = _.value
  implicit val encoderDomain: Encoder[Domain] = Encoder.encodeString.contramap(_.value)
  implicit val decoderDomain: Decoder[Domain] = Decoder.decodeString.map(Domain(_))
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

final case class UpTime(value: Duration) extends AnyVal
object UpTime {
  implicit val showUpTime: Show[UpTime] = ut => durationFormatter.format(ut.value)
  implicit val encoderUpTime: Encoder[UpTime] = Encoder.encodeDuration.contramap(_.value)
  implicit val decoderUpTime: Decoder[UpTime] = Decoder.decodeDuration.map(UpTime(_))
}

final case class TimeZone(value: ZoneId) extends AnyVal
object TimeZone {
  implicit val showTimeZone: Show[TimeZone] = _.value.toString
  implicit val encoderTimeZone: Encoder[TimeZone] = Encoder.encodeZoneId.contramap(_.value)
  implicit val decoderTimeZone: Decoder[TimeZone] = Decoder.decodeZoneId.map(TimeZone(_))
}
