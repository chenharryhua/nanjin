package com.github.chenharryhua.nanjin.guard.config

import enumeratum.EnumEntry.Lowercase
import enumeratum.{CatsEnum, CirceEnum, Enum, EnumEntry}

import scala.collection.immutable

sealed abstract class Importance(val value: Int) extends EnumEntry with Lowercase {
  def isFireStartEvent: Boolean
  def isFireSuccEvent: Boolean
}

object Importance extends CatsEnum[Importance] with Enum[Importance] with CirceEnum[Importance] {
  override def values: immutable.IndexedSeq[Importance] = findValues

  case object SystemEvent extends Importance(50) {
    override val isFireStartEvent: Boolean = true
    override val isFireSuccEvent: Boolean  = true
  }
  case object Critical extends Importance(40) {
    override val isFireStartEvent: Boolean = true
    override val isFireSuccEvent: Boolean  = true
  }
  case object High extends Importance(30) {
    override val isFireStartEvent: Boolean = true
    override val isFireSuccEvent: Boolean  = true
  }
  case object Medium extends Importance(20) {
    override val isFireStartEvent: Boolean = false
    override val isFireSuccEvent: Boolean  = true
  }
  case object Low extends Importance(10) {
    override val isFireStartEvent: Boolean = false
    override val isFireSuccEvent: Boolean  = false
  }
}
