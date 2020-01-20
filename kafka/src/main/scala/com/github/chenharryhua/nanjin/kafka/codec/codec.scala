package com.github.chenharryhua.nanjin.kafka

package object codec {
  object eq extends EqMessage

  sealed trait KeyValueTag

  object KeyValueTag {
    case object KeyTag extends KeyValueTag
    case object ValueTag extends KeyValueTag
  }
}
