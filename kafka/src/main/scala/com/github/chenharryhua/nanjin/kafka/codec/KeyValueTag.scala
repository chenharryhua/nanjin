package com.github.chenharryhua.nanjin.kafka.codec

import cats.instances.int.catsKernelStdOrderForInt
import enumeratum.values.{CatsOrderValueEnum, IntEnum, IntEnumEntry}
import monocle.Prism
import monocle.macros.GenPrism

import scala.collection.immutable

sealed abstract class KeyValueTag(val value: Int, val name: String)
    extends IntEnumEntry with Serializable

object KeyValueTag extends CatsOrderValueEnum[Int, KeyValueTag] with IntEnum[KeyValueTag] {
  override val values: immutable.IndexedSeq[KeyValueTag] = findValues

  case object KeyTag extends KeyValueTag(0, "key")
  case object ValueTag extends KeyValueTag(1, "value")

  type KeyTag   = KeyTag.type
  type ValueTag = ValueTag.type

  implicit val keyTagPrism: Prism[KeyValueTag, KeyTag] =
    GenPrism[KeyValueTag, KeyTag]

  implicit val valueTagPrism: Prism[KeyValueTag, ValueTag] =
    GenPrism[KeyValueTag, ValueTag]

}
