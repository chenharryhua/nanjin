package com.github.chenharryhua.nanjin.guard.config

import enumeratum.EnumEntry.Lowercase
import enumeratum.values.{CatsOrderValueEnum, IntCirceEnum, IntEnum, IntEnumEntry}
import enumeratum.{CatsEnum, CirceEnum, Enum, EnumEntry}

import scala.collection.immutable

sealed abstract class Importance(val value: Int) extends IntEnumEntry with Lowercase

object Importance extends CatsOrderValueEnum[Int, Importance] with IntEnum[Importance] with IntCirceEnum[Importance] {
  override val values: immutable.IndexedSeq[Importance] = findValues

  case object Critical extends Importance(40)
  case object High extends Importance(30)
  case object Medium extends Importance(20)
  case object Low extends Importance(10)
}

sealed trait CountAction extends EnumEntry with Lowercase
object CountAction extends CatsEnum[CountAction] with Enum[CountAction] with CirceEnum[CountAction] {
  override val values: IndexedSeq[CountAction] = findValues
  case object Yes extends CountAction
  case object No extends CountAction
}

sealed trait TimeAction extends EnumEntry with Lowercase
object TimeAction extends CatsEnum[TimeAction] with Enum[TimeAction] with CirceEnum[TimeAction] {
  override val values: IndexedSeq[TimeAction] = findValues
  case object Yes extends TimeAction
  case object No extends TimeAction
}

sealed trait ActionTermination extends EnumEntry with Lowercase
object ActionTermination
    extends CatsEnum[ActionTermination] with Enum[ActionTermination] with CirceEnum[ActionTermination] {
  override val values: IndexedSeq[ActionTermination] = findValues
  case object Yes extends ActionTermination
  case object No extends ActionTermination
}
