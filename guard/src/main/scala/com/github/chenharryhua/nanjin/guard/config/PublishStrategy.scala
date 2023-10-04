package com.github.chenharryhua.nanjin.guard.config

import enumeratum.values.{CatsOrderValueEnum, IntCirceEnum, IntEnum, IntEnumEntry}
import enumeratum.{CatsEnum, CirceEnum, Enum, EnumEntry}
import io.circe.Json

sealed abstract class PublishStrategy(override val entryName: String) extends EnumEntry with Product

object PublishStrategy
    extends Enum[PublishStrategy] with CirceEnum[PublishStrategy] with CatsEnum[PublishStrategy] {
  override val values: IndexedSeq[PublishStrategy] = findValues

  case object Bipartite extends PublishStrategy("bipartite") // publish start and done event
  case object Unipartite extends PublishStrategy("unipartite") // publish done event
  case object Silent extends PublishStrategy("silent") // publish nothing
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

final private[guard] case class ServiceName(value: String) extends AnyVal
final private[guard] case class ServiceBrief(value: Option[Json]) extends AnyVal

final private[guard] case class ActionName(value: String) extends AnyVal
final private[guard] case class Measurement(value: String) extends AnyVal
