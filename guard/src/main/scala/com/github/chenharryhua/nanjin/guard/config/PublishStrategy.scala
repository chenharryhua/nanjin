package com.github.chenharryhua.nanjin.guard.config

import enumeratum.values.{CatsOrderValueEnum, IntCirceEnum, IntEnum, IntEnumEntry}
import enumeratum.{CatsEnum, CirceEnum, Enum, EnumEntry}
import retry.RetryPolicy

sealed trait PublishStrategy extends EnumEntry with Product

object PublishStrategy
    extends Enum[PublishStrategy] with CirceEnum[PublishStrategy] with CatsEnum[PublishStrategy] {
  override val values: IndexedSeq[PublishStrategy] = findValues

  case object StartAndComplete extends PublishStrategy
  case object CompleteOnly extends PublishStrategy
  case object Silent extends PublishStrategy
}

sealed abstract class Importance(override val value: Int, val entryName: String)
    extends IntEnumEntry with Product

object Importance
    extends CatsOrderValueEnum[Int, Importance] with IntEnum[Importance] with IntCirceEnum[Importance] {
  override val values: IndexedSeq[Importance] = findValues

  case object Critical extends Importance(3, "critical")
  case object Normal extends Importance(2, "normal")
  case object Trivial extends Importance(1, "trivial")
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
final private[guard] case class ActionName(value: String) extends AnyVal
final private[guard] case class Measurement(value: String) extends AnyVal
final private[guard] case class Policy private (value: String) extends AnyVal
private[guard] object Policy {
  def apply[F[_]](rp: RetryPolicy[F]): Policy = Policy(rp.show)
}
