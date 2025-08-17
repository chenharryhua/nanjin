package com.github.chenharryhua.nanjin.guard.config

import cats.Show
import enumeratum.values.{CatsOrderValueEnum, IntCirceEnum, IntEnum, IntEnumEntry}
import enumeratum.{CirceEnum, Enum, EnumEntry}
import io.circe.{Decoder, Encoder, Json}

sealed abstract class AlarmLevel(override val value: Int, val entryName: String)
    extends IntEnumEntry with Product

object AlarmLevel
    extends CatsOrderValueEnum[Int, AlarmLevel] with IntEnum[AlarmLevel] with IntCirceEnum[AlarmLevel] {
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

  case object Console extends LogFormat
  case object PlainText extends LogFormat
  case object JsonNoSpaces extends LogFormat
  case object JsonSpaces2 extends LogFormat
  case object JsonVerbose extends LogFormat
}

final case class TaskName(value: String) extends AnyVal
object TaskName {
  implicit val showTaskName: Show[TaskName] = _.value
  implicit val encoderTaskName: Encoder[TaskName] = Encoder.encodeString.contramap(_.value)
  implicit val decoderTaskName: Decoder[TaskName] = Decoder.decodeString.map(TaskName(_))
}

final case class ServiceName(value: String) extends AnyVal
object ServiceName {
  implicit val showServiceName: Show[ServiceName] = _.value
  implicit val encoderServiceName: Encoder[ServiceName] = Encoder.encodeString.contramap(_.value)
  implicit val decoderServiceName: Decoder[ServiceName] = Decoder.decodeString.map(ServiceName(_))
}

final case class HomePage(value: String) extends AnyVal
object HomePage {
  implicit val showHomePage: Show[HomePage] = _.value
  implicit val encoderHomePage: Encoder[HomePage] = Encoder.encodeString.contramap(_.value)
  implicit val decoderHomePage: Decoder[HomePage] = Decoder.decodeString.map(HomePage(_))
}

final case class Domain(value: String) extends AnyVal
object Domain {
  implicit val showDomain: Show[Domain] = _.value
  implicit val encoderDomain: Encoder[Domain] = Encoder.encodeString.contramap(_.value)
  implicit val decoderDomain: Decoder[Domain] = Decoder.decodeString.map(Domain(_))
}

final private[guard] case class ServiceBrief(value: Json) extends AnyVal
