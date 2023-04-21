package com.github.chenharryhua.nanjin.guard.config

import cats.Order
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

sealed abstract class AlertLevel(override val entryName: String, val value: Int)
    extends EnumEntry with Product

object AlertLevel extends Enum[AlertLevel] with CirceEnum[AlertLevel] with CatsEnum[AlertLevel] {
  override val values: IndexedSeq[AlertLevel] = findValues

  case object Error extends AlertLevel("error", 30)
  case object Warn extends AlertLevel("warn", 20)
  case object Info extends AlertLevel("info", 10)

  implicit final val orderingAlertLevel: Ordering[AlertLevel] = Ordering.by[AlertLevel, Int](_.value)
  implicit final val orderAlertLevel: Order[AlertLevel]       = Order.fromOrdering[AlertLevel]
}

final private[guard] case class ServiceName(value: String) extends AnyVal
final private[guard] case class ActionName(value: String) extends AnyVal
final private[guard] case class Measurement(value: String) extends AnyVal
final private[guard] case class Policy private (value: String) extends AnyVal
private[guard] object Policy {
  def apply[F[_]](rp: RetryPolicy[F]): Policy = Policy(rp.show)
}
