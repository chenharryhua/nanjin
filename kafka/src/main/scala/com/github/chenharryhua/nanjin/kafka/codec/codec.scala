package com.github.chenharryhua.nanjin.kafka

import enumeratum._

import scala.collection.immutable

package object codec {
  object eq extends EqMessage

  sealed abstract class KeyValueTag(override val entryName: String) extends EnumEntry

  object KeyValueTag extends Enum[KeyValueTag] {
    val values: immutable.IndexedSeq[KeyValueTag] = findValues

    case object KeyTag extends KeyValueTag("key")
    case object ValueTag extends KeyValueTag("value")
  }
}
