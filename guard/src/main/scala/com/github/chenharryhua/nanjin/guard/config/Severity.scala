package com.github.chenharryhua.nanjin.guard.config

import com.github.chenharryhua.nanjin.common.NJLogLevel
import enumeratum.EnumEntry.Lowercase
import enumeratum.values.{CatsOrderValueEnum, IntCirceEnum, IntEnum, IntEnumEntry}

import scala.collection.immutable

// https://en.wikipedia.org/wiki/Syslog
sealed abstract class Severity(val value: Int, val color: String, val logLevel: NJLogLevel)
    extends IntEnumEntry with Lowercase

object Severity extends CatsOrderValueEnum[Int, Severity] with IntEnum[Severity] with IntCirceEnum[Severity] {
  override def values: immutable.IndexedSeq[Severity] = findValues
  case object Emergency extends Severity(0, "#ff0000", NJLogLevel.FATAL)
  case object Alert extends Severity(1, "#cc0000", NJLogLevel.ERROR)
  case object Critical extends Severity(2, "#990000", NJLogLevel.ERROR)
  case object Error extends Severity(3, "danger", NJLogLevel.ERROR)
  case object Warning extends Severity(4, "#c39002", NJLogLevel.WARN)
  case object Notice extends Severity(5, "#dca302", NJLogLevel.WARN)
  case object Informational extends Severity(6, "#b3d1ff", NJLogLevel.INFO)
  case object Debug extends Severity(7, "#8fa7cc", NJLogLevel.DEBUG)
  case object Success extends Severity(8, "good", NJLogLevel.TRACE)
}
