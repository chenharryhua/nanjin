package com.github.chenharryhua.nanjin.guard.config

import enumeratum.EnumEntry.Lowercase
import enumeratum.values.{CatsOrderValueEnum, IntCirceEnum, IntEnum, IntEnumEntry}

import scala.collection.immutable

// https://en.wikipedia.org/wiki/Syslog
sealed abstract class Severity(val value: Int, val color: String) extends IntEnumEntry with Lowercase

object Severity extends CatsOrderValueEnum[Int, Severity] with IntEnum[Severity] with IntCirceEnum[Severity] {
  override def values: immutable.IndexedSeq[Severity] = findValues
  case object Emergency extends Severity(0, "#ff0000")
  case object Alert extends Severity(1, "#cc0000")
  case object Critical extends Severity(2, "#990000")
  case object Error extends Severity(3, "danger")
  case object Warning extends Severity(4, "#c39002")
  case object Notice extends Severity(5, "#dca302")
  case object Informational extends Severity(6, "#b3d1ff")
  case object Debug extends Severity(7, "#8fa7cc")
  case object Good extends Severity(8, "good")
}
