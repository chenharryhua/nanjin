package com.github.chenharryhua.nanjin.guard.config

import cats.Order
import enumeratum.EnumEntry.Lowercase
import enumeratum.{CatsEnum, CirceEnum, Enum, EnumEntry}

sealed trait Importance extends EnumEntry with Lowercase with Product

object Importance extends CatsEnum[Importance] with Enum[Importance] with CirceEnum[Importance] {
  override val values: IndexedSeq[Importance] = findValues

  case object Critical extends Importance
  case object Notice extends Importance
  case object Aware extends Importance
  case object Silent extends Importance
}

sealed abstract class AlertLevel(override val entryName: String, val value: Int)
    extends EnumEntry with Product

object AlertLevel extends CatsEnum[AlertLevel] with Enum[AlertLevel] with CirceEnum[AlertLevel] {
  override val values: IndexedSeq[AlertLevel] = findValues

  case object Error extends AlertLevel("error", 30)
  case object Warn extends AlertLevel("warn", 20)
  case object Info extends AlertLevel("info", 10)

  implicit final val orderingImportance: Ordering[AlertLevel] = Ordering.by[AlertLevel, Int](_.value)
  implicit final val orderImportance: Order[AlertLevel]       = Order.fromOrdering[AlertLevel]
}
