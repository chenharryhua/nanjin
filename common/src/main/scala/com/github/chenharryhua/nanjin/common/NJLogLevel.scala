package com.github.chenharryhua.nanjin.common

import enumeratum.values.{CatsOrderValueEnum, IntCirceEnum, IntEnum, IntEnumEntry}
import org.typelevel.log4cats.extras.LogLevel

import scala.collection.immutable

sealed abstract class NJLogLevel(override val value: Int, val logLevel: LogLevel)
    extends IntEnumEntry with Product with Serializable

object NJLogLevel
    extends CatsOrderValueEnum[Int, NJLogLevel] with IntEnum[NJLogLevel] with IntCirceEnum[NJLogLevel] {
  override val values: immutable.IndexedSeq[NJLogLevel] = findValues

  case object ALL extends NJLogLevel(1, LogLevel.Trace)
  case object TRACE extends NJLogLevel(2, LogLevel.Trace)
  case object DEBUG extends NJLogLevel(3, LogLevel.Debug)
  case object INFO extends NJLogLevel(4, LogLevel.Info)
  case object WARN extends NJLogLevel(5, LogLevel.Warn)
  case object ERROR extends NJLogLevel(6, LogLevel.Error)
  case object FATAL extends NJLogLevel(7, LogLevel.Error)
  case object OFF extends NJLogLevel(8, LogLevel.Error)
}
