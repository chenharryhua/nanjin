package com.github.chenharryhua.nanjin.common

import enumeratum.{CatsEnum, CirceEnum, Enum, EnumEntry}

import scala.collection.immutable

sealed abstract class NJLogLevel(val value: Int, val logLevel: org.log4s.LogLevel)
    extends EnumEntry with Ordered[NJLogLevel] with Serializable {
  final override def compare(that: NJLogLevel): Int = Integer.compare(value, that.value)
}

object NJLogLevel extends Enum[NJLogLevel] with CatsEnum[NJLogLevel] with CirceEnum[NJLogLevel] {
  override val values: immutable.IndexedSeq[NJLogLevel] = findValues

  case object ALL extends NJLogLevel(1, org.log4s.Trace)
  case object TRACE extends NJLogLevel(2, org.log4s.Trace)
  case object DEBUG extends NJLogLevel(3, org.log4s.Debug)
  case object INFO extends NJLogLevel(4, org.log4s.Info)
  case object WARN extends NJLogLevel(5, org.log4s.Warn)
  case object ERROR extends NJLogLevel(6, org.log4s.Error)
  case object FATAL extends NJLogLevel(7, org.log4s.Error)
  case object OFF extends NJLogLevel(8, org.log4s.Error)
}
