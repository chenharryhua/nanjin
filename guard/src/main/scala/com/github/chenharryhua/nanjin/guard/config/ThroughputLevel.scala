package com.github.chenharryhua.nanjin.guard.config

import enumeratum.EnumEntry.Lowercase
import enumeratum.{CatsEnum, CirceEnum, Enum, EnumEntry}

import scala.collection.immutable

sealed abstract class ThroughputLevel(val value: Int) extends EnumEntry with Lowercase {
  def isFireStartEvent: Boolean
  def isFireSuccEvent: Boolean
}

object ThroughputLevel extends CatsEnum[ThroughputLevel] with Enum[ThroughputLevel] with CirceEnum[ThroughputLevel] {
  override def values: immutable.IndexedSeq[ThroughputLevel] = findValues

  case object High extends ThroughputLevel(30) {
    override val isFireStartEvent: Boolean = true
    override val isFireSuccEvent: Boolean  = true
  }
  case object Medium extends ThroughputLevel(20) {
    override val isFireStartEvent: Boolean = false
    override val isFireSuccEvent: Boolean  = true
  }
  case object Low extends ThroughputLevel(10) {
    override val isFireStartEvent: Boolean = false
    override val isFireSuccEvent: Boolean  = false
  }
}
