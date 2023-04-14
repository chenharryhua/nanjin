package com.github.chenharryhua.nanjin.guard.config

import cats.Order
import enumeratum.EnumEntry.Lowercase
import enumeratum.{CatsEnum, CirceEnum, Enum, EnumEntry}

sealed trait Importance extends EnumEntry with Lowercase with Product

object Importance extends Enum[Importance] with CirceEnum[Importance] with CatsEnum[Importance] {
  override val values: IndexedSeq[Importance] = findValues

  case object Critical extends Importance
  case object Notice extends Importance
  case object Aware extends Importance
  case object Silent extends Importance
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
