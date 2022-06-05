package com.github.chenharryhua.nanjin.common

import cats.kernel.Order
import enumeratum.{CatsEnum, CirceEnum, Enum, EnumEntry}
import org.typelevel.log4cats.extras.LogLevel

import scala.collection.immutable

sealed abstract class NJLogLevel(val value: Int, val logLevel: LogLevel) extends EnumEntry with Serializable

object NJLogLevel extends Enum[NJLogLevel] with CirceEnum[NJLogLevel] with CatsEnum[NJLogLevel] {
  override val values: immutable.IndexedSeq[NJLogLevel] = findValues

  case object ALL extends NJLogLevel(1, LogLevel.Trace)
  case object TRACE extends NJLogLevel(2, LogLevel.Trace)
  case object DEBUG extends NJLogLevel(3, LogLevel.Debug)
  case object INFO extends NJLogLevel(4, LogLevel.Info)
  case object WARN extends NJLogLevel(5, LogLevel.Warn)
  case object ERROR extends NJLogLevel(6, LogLevel.Error)
  case object FATAL extends NJLogLevel(7, LogLevel.Error)
  case object OFF extends NJLogLevel(8, LogLevel.Error)

  implicit final val orderingNJLogLevel: Ordering[NJLogLevel] =
    Ordering.by[NJLogLevel, Int](_.value)

  implicit final val orderNJLogLevel: Order[NJLogLevel] = Order.fromOrdering[NJLogLevel]
}
