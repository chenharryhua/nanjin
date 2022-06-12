package com.github.chenharryhua.nanjin.messages.kafka

import enumeratum.values.{CatsValueEnum, IntEnum, IntEnumEntry}
import monocle.Prism
import monocle.macros.GenPrism

import scala.collection.immutable

sealed abstract class KeyValueTag(override val value: Int, val name: String, val isKey: Boolean)
    extends IntEnumEntry with Product with Serializable

object KeyValueTag extends CatsValueEnum[Int, KeyValueTag] with IntEnum[KeyValueTag] {
  override val values: immutable.IndexedSeq[KeyValueTag] = findValues

  case object Key extends KeyValueTag(0, "Key", isKey = true)
  case object Value extends KeyValueTag(1, "Value", isKey = false)

  type Key   = Key.type
  type Value = Value.type

  implicit val keyTagPrism: Prism[KeyValueTag, Key] =
    GenPrism[KeyValueTag, Key]

  implicit val valueTagPrism: Prism[KeyValueTag, Value] =
    GenPrism[KeyValueTag, Value]

}
