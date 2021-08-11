package com.github.chenharryhua.nanjin.guard.config

import enumeratum.EnumEntry.Lowercase
import enumeratum.values.{CatsOrderValueEnum, IntCirceEnum, IntEnum, IntEnumEntry}

import scala.collection.immutable

// https://en.wikipedia.org/wiki/Syslog
sealed abstract class Severity(val value: Int) extends IntEnumEntry with Lowercase

object Severity extends CatsOrderValueEnum[Int, Severity] with IntEnum[Severity] with IntCirceEnum[Severity] {
  override def values: immutable.IndexedSeq[Severity] = findValues

  case object SystemEvent extends Severity(0)
  case object Critical extends Severity(1)
  case object Error extends Severity(2)
  case object Notice extends Severity(3)
}
