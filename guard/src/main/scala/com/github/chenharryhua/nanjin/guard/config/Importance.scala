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

sealed trait MetricSnapshotType extends EnumEntry
object MetricSnapshotType
    extends CatsEnum[MetricSnapshotType] with Enum[MetricSnapshotType] with CirceEnum[MetricSnapshotType] {
  override val values: IndexedSeq[MetricSnapshotType] = findValues
  case object Full extends MetricSnapshotType // == MetricFilter.ALL
  case object Regular extends MetricSnapshotType // filter out zero
  case object Delta extends MetricSnapshotType // filter out unchanged and zero
}
