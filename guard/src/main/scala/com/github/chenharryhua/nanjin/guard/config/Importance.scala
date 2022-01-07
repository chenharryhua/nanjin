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

sealed trait CountAction extends EnumEntry
object CountAction extends CatsEnum[CountAction] with Enum[CountAction] with CirceEnum[CountAction] {
  override val values: IndexedSeq[CountAction] = findValues
  case object Yes extends CountAction
  case object No extends CountAction
}

sealed trait TimeAction extends EnumEntry
object TimeAction extends CatsEnum[TimeAction] with Enum[TimeAction] with CirceEnum[TimeAction] {
  override val values: IndexedSeq[TimeAction] = findValues
  case object Yes extends TimeAction
  case object No extends TimeAction
}

sealed trait ExpensiveAction extends EnumEntry
object ExpensiveAction extends CatsEnum[ExpensiveAction] with Enum[ExpensiveAction] with CirceEnum[ExpensiveAction] {
  override val values: IndexedSeq[ExpensiveAction] = findValues
  case object Yes extends ExpensiveAction
  case object No extends ExpensiveAction
}

sealed trait MetricSnapshotType extends EnumEntry
object MetricSnapshotType
    extends CatsEnum[MetricSnapshotType] with Enum[MetricSnapshotType] with CirceEnum[MetricSnapshotType] {
  override val values: IndexedSeq[MetricSnapshotType] = findValues
  case object Full extends MetricSnapshotType // == MetricFilter.ALL
  case object Regular extends MetricSnapshotType // filter out zero
  case object Delta extends MetricSnapshotType // filter out unchanged and zero
}
