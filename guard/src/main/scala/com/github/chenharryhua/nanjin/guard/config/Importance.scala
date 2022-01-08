package com.github.chenharryhua.nanjin.guard.config

import enumeratum.values.*
import enumeratum.{CatsEnum, CirceEnum, Enum, EnumEntry}

import scala.collection.immutable

sealed abstract class Importance(val value: Int) extends IntEnumEntry

object Importance
    extends CatsOrderValueEnum[Int, Importance] with IntEnum[Importance] with IntCirceEnum[Importance]
    with CatsValueEnum[Int, Importance] {
  override val values: immutable.IndexedSeq[Importance] = findValues

  case object Critical extends Importance(40)
  case object High extends Importance(30)
  case object Medium extends Importance(20)
  case object Low extends Importance(10)
}

sealed trait CountAction extends EnumEntry {
  def value: Boolean
}
object CountAction extends CatsEnum[CountAction] with Enum[CountAction] with CirceEnum[CountAction] {
  override val values: IndexedSeq[CountAction] = findValues
  case object Yes extends CountAction {
    override val value: Boolean = true
  }
  case object No extends CountAction {
    override val value: Boolean = false
  }
}

sealed trait TimeAction extends EnumEntry {
  def value: Boolean
}
object TimeAction extends CatsEnum[TimeAction] with Enum[TimeAction] with CirceEnum[TimeAction] {
  override val values: IndexedSeq[TimeAction] = findValues
  case object Yes extends TimeAction {
    override val value: Boolean = true
  }
  case object No extends TimeAction {
    override val value: Boolean = false
  }
}

sealed trait ExpensiveAction extends EnumEntry {
  def value: Boolean
}
object ExpensiveAction extends CatsEnum[ExpensiveAction] with Enum[ExpensiveAction] with CirceEnum[ExpensiveAction] {
  override val values: IndexedSeq[ExpensiveAction] = findValues
  case object Yes extends ExpensiveAction {
    override val value: Boolean = true
  }
  case object No extends ExpensiveAction {
    override val value: Boolean = false
  }
}

sealed trait MetricSnapshotType extends EnumEntry
object MetricSnapshotType
    extends CatsEnum[MetricSnapshotType] with Enum[MetricSnapshotType] with CirceEnum[MetricSnapshotType] {
  override val values: IndexedSeq[MetricSnapshotType] = findValues
  case object Full extends MetricSnapshotType // == MetricFilter.ALL
  case object Regular extends MetricSnapshotType // filter out zero
  case object Delta extends MetricSnapshotType // filter out unchanged and zero
}
