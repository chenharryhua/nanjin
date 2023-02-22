package com.github.chenharryhua.nanjin.guard.config

import cats.Order
import enumeratum.{CatsEnum, CirceEnum, Enum, EnumEntry}

sealed abstract class Importance(val value: Int) extends EnumEntry with Product

object Importance extends CatsEnum[Importance] with Enum[Importance] with CirceEnum[Importance] {
  override val values: IndexedSeq[Importance] = findValues

  case object Critical extends Importance(50)
  case object Notice extends Importance(40)
  case object Aware extends Importance(30)
  case object Silent extends Importance(20)
  case object Trivial extends Importance(10)

  implicit final val orderingImportance: Ordering[Importance] = Ordering.by[Importance, Int](_.value)
  implicit final val orderImportance: Order[Importance]       = Order.fromOrdering[Importance]
}

sealed abstract class AlertLevel(val value: Int) extends EnumEntry with Product

object AlertLevel extends CatsEnum[AlertLevel] with Enum[AlertLevel] with CirceEnum[AlertLevel] {
  override val values: IndexedSeq[AlertLevel] = findValues

  case object Error extends AlertLevel(30)
  case object Warn extends AlertLevel(20)
  case object Info extends AlertLevel(10)

  implicit final val orderingImportance: Ordering[AlertLevel] = Ordering.by[AlertLevel, Int](_.value)
  implicit final val orderImportance: Order[AlertLevel]       = Order.fromOrdering[AlertLevel]
}
