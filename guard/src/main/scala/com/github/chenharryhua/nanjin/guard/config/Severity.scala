package com.github.chenharryhua.nanjin.guard.config

import enumeratum.EnumEntry.Lowercase
import enumeratum.{CatsEnum, CirceEnum, Enum, EnumEntry}

import scala.collection.immutable

// https://en.wikipedia.org/wiki/Syslog
sealed abstract class Severity(val value: Int) extends EnumEntry with Lowercase

object Severity extends CatsEnum[Severity] with Enum[Severity] with CirceEnum[Severity] {
  override def values: immutable.IndexedSeq[Severity] = findValues

  case object Essential extends Severity(0)
  case object Critical extends Severity(1)
  case object Error extends Severity(2)
  case object Notice extends Severity(3)
}
