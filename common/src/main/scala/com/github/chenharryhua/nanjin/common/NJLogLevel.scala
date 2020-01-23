package com.github.chenharryhua.nanjin.common

import enumeratum.{CatsEnum, Enum, EnumEntry}

import scala.collection.immutable

sealed abstract class NJLogLevel(val value: Int)
    extends EnumEntry with Ordered[NJLogLevel] with Serializable {
  final override def compare(that: NJLogLevel): Int = value - that.value
}

object NJLogLevel extends Enum[NJLogLevel] with CatsEnum[NJLogLevel] {
  override val values: immutable.IndexedSeq[NJLogLevel] = findValues

  case object ALL extends NJLogLevel(1)
  case object TRACE extends NJLogLevel(2)
  case object DEBUG extends NJLogLevel(3)
  case object INFO extends NJLogLevel(4)
  case object WARN extends NJLogLevel(5)
  case object ERROR extends NJLogLevel(6)
  case object FATAL extends NJLogLevel(7)
  case object OFF extends NJLogLevel(8)
}
