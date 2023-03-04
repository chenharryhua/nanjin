package com.github.chenharryhua.nanjin.guard.config

import cats.Order
import enumeratum.{CatsEnum, CirceEnum, Enum, EnumEntry}

sealed abstract class Importance(val value: Boolean) extends EnumEntry with Product {
  def isPublishActionStart: Boolean
  def isPublishActionComplete: Boolean
}

object Importance extends CatsEnum[Importance] with Enum[Importance] with CirceEnum[Importance] {
  override val values: IndexedSeq[Importance] = findValues

  case object Critical extends Importance(true) {
    override val isPublishActionStart: Boolean    = true
    override val isPublishActionComplete: Boolean = true
  }
  case object Notice extends Importance(false) {
    override val isPublishActionStart: Boolean    = true
    override val isPublishActionComplete: Boolean = true
  }
  case object Aware extends Importance(false) {
    override val isPublishActionStart: Boolean    = false
    override val isPublishActionComplete: Boolean = true
  }
  case object Silent extends Importance(false) {
    override val isPublishActionStart: Boolean    = false
    override val isPublishActionComplete: Boolean = false
  }
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
