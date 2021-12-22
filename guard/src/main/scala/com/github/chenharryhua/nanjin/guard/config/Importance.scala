package com.github.chenharryhua.nanjin.guard.config

import enumeratum.EnumEntry.Lowercase
import enumeratum.{CatsEnum, CirceEnum, Enum, EnumEntry}

import scala.collection.immutable

sealed abstract class Importance(val value: Int) extends EnumEntry with Lowercase {}

object Importance extends CatsEnum[Importance] with Enum[Importance] with CirceEnum[Importance] {
  override def values: immutable.IndexedSeq[Importance] = findValues

  case object Critical extends Importance(40) {} // slacking
  case object High extends Importance(30) {} // logging
  case object Medium extends Importance(20) {} // timing
  case object Low extends Importance(10) {} // ignore
}
