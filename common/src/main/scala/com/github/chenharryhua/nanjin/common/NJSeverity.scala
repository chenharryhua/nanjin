package com.github.chenharryhua.nanjin.common

import enumeratum.{CatsEnum, CirceEnum, Enum, EnumEntry}

import scala.collection.immutable

// https://en.wikipedia.org/wiki/Syslog
sealed abstract class NJSeverity(val value: Int) extends EnumEntry with Ordered[NJSeverity] {
  final override def compare(that: NJSeverity): Int = Integer.compare(value, that.value)
}

object NJSeverity extends Enum[NJSeverity] with CatsEnum[NJSeverity] with CirceEnum[NJSeverity] {
  override def values: immutable.IndexedSeq[NJSeverity] = findValues
  case object Emergency extends NJSeverity(0)
  case object Alert extends NJSeverity(1)
  case object Critical extends NJSeverity(2)
  case object Error extends NJSeverity(3)
  case object Warning extends NJSeverity(4)
  case object Notice extends NJSeverity(5)
  case object Informational extends NJSeverity(6)
  case object Debug extends NJSeverity(7)
}
