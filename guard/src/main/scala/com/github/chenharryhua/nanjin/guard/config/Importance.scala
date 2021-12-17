package com.github.chenharryhua.nanjin.guard.config

import cats.Show
import com.amazonaws.thirdparty.apache.codec.digest.DigestUtils
import enumeratum.EnumEntry.Lowercase
import enumeratum.{CatsEnum, CirceEnum, Enum, EnumEntry}
import io.circe.generic.JsonCodec

import scala.collection.immutable

sealed abstract class Importance(val value: Int) extends EnumEntry with Lowercase {}

object Importance extends CatsEnum[Importance] with Enum[Importance] with CirceEnum[Importance] {
  override def values: immutable.IndexedSeq[Importance] = findValues

  case object Critical extends Importance(40) {} // slacking
  case object High extends Importance(30) {} // logging
  case object Medium extends Importance(20) {} // timing
  case object Low extends Importance(10) {} // do nothing
}

@JsonCodec
final case class GuardId(prefix: String, value: String) {
  val sha1Hex: String           = DigestUtils.sha1Hex(s"$prefix/$value")
  val displayName: String       = s"$value/${sha1Hex.take(8)}"
  override val toString: String = displayName
}
object GuardId {
  implicit val showGuardId: Show[GuardId] = _.displayName
}
