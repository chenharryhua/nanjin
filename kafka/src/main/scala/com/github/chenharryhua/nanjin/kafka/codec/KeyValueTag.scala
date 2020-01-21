package com.github.chenharryhua.nanjin.kafka.codec

import cats.instances.int.catsKernelStdOrderForInt
import enumeratum.values.{CatsOrderValueEnum, IntEnum, IntEnumEntry}
import monocle.Prism
import monocle.macros.GenPrism

import scala.collection.immutable

sealed abstract class KeyValueTag(val value: Int, val name: String, val isKey: Boolean)
    extends IntEnumEntry with Serializable

object KeyValueTag extends CatsOrderValueEnum[Int, KeyValueTag] with IntEnum[KeyValueTag] {
  override val values: immutable.IndexedSeq[KeyValueTag] = findValues

  case object Key extends KeyValueTag(0, "key", isKey     = true)
  case object Value extends KeyValueTag(1, "value", isKey = false)

  type Key   = Key.type
  type Value = Value.type

  implicit val keyTagPrism: Prism[KeyValueTag, Key] =
    GenPrism[KeyValueTag, Key]

  implicit val valueTagPrism: Prism[KeyValueTag, Value] =
    GenPrism[KeyValueTag, Value]

}
