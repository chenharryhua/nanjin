package com.github.chenharryhua.nanjin.guard.config

import cats.Show
import enumeratum.values.{CatsOrderValueEnum, IntCirceEnum, IntEnum, IntEnumEntry}
import io.circe.{Decoder, Encoder, Json}

sealed abstract class PublishStrategy(override val value: Int, val entryName: String)
    extends IntEnumEntry with Product

object PublishStrategy
    extends CatsOrderValueEnum[Int, PublishStrategy] with IntCirceEnum[PublishStrategy]
    with IntEnum[PublishStrategy] {
  override val values: IndexedSeq[PublishStrategy] = findValues

  case object Bipartite extends PublishStrategy(2, "bipartite") // publish start and done event
  case object Unipartite extends PublishStrategy(1, "unipartite") // publish done event
  case object Silent extends PublishStrategy(0, "silent") // publish nothing
}

sealed abstract class Importance(override val value: Int, val entryName: String)
    extends IntEnumEntry with Product

object Importance
    extends CatsOrderValueEnum[Int, Importance] with IntEnum[Importance] with IntCirceEnum[Importance] {
  override val values: IndexedSeq[Importance] = findValues

  case object Critical extends Importance(4, "critical")
  case object Normal extends Importance(3, "normal")
  case object Insignificant extends Importance(2, "insignificant")
  case object Suppressed extends Importance(1, "suppressed")
}

sealed abstract class AlertLevel(override val value: Int, val entryName: String)
    extends IntEnumEntry with Product

object AlertLevel
    extends CatsOrderValueEnum[Int, AlertLevel] with IntEnum[AlertLevel] with IntCirceEnum[AlertLevel] {
  override val values: IndexedSeq[AlertLevel] = findValues

  case object Error extends AlertLevel(3, "error")
  case object Warn extends AlertLevel(2, "warn")
  case object Info extends AlertLevel(1, "info")
}

final case class TaskName(value: String) extends AnyVal
object TaskName {
  implicit val showTaskName: Show[TaskName]       = _.value
  implicit val encoderTaskName: Encoder[TaskName] = Encoder.encodeString.contramap(_.value)
  implicit val decoderTaskName: Decoder[TaskName] = Decoder.decodeString.map(TaskName(_))
}

final case class ServiceName(value: String) extends AnyVal
object ServiceName {
  implicit val showServiceName: Show[ServiceName]       = _.value
  implicit val encoderServiceName: Encoder[ServiceName] = Encoder.encodeString.contramap(_.value)
  implicit val decoderServiceName: Decoder[ServiceName] = Decoder.decodeString.map(ServiceName(_))
}

final case class HomePage(value: String) extends AnyVal
object HomePage {
  implicit val showHomePage: Show[HomePage]       = _.value
  implicit val encoderHomePage: Encoder[HomePage] = Encoder.encodeString.contramap(_.value)
  implicit val decoderHomePage: Decoder[HomePage] = Decoder.decodeString.map(HomePage(_))
}

final case class ActionName(value: String) extends AnyVal
object ActionName {
  implicit val showActionName: Show[ActionName]       = _.value
  implicit val encoderActionName: Encoder[ActionName] = Encoder.encodeString.contramap(_.value)
  implicit val decoderActionName: Decoder[ActionName] = Decoder.decodeString.map(ActionName(_))
}

final case class Measurement(value: String) extends AnyVal
object Measurement {
  implicit val showMeasurement: Show[Measurement]       = _.value
  implicit val encoderMeasurement: Encoder[Measurement] = Encoder.encodeString.contramap(_.value)
  implicit val decoderMeasurement: Decoder[Measurement] = Decoder.decodeString.map(Measurement(_))
}

final private[guard] case class ServiceBrief(value: Json) extends AnyVal
