package com.github.chenharryhua.nanjin.messages

import io.scalaland.chimney.Transformer
import org.apache.kafka.common.record.TimestampType

package object kafka {
  object instances extends BitraverseKafkaRecord with EqMessage with Isos

  implicit private[kafka] val timestampTypeTransformer: Transformer[Int, TimestampType] = {
    case 0 => TimestampType.CREATE_TIME
    case 1 => TimestampType.LOG_APPEND_TIME
    case _ => TimestampType.NO_TIMESTAMP_TYPE
  }
}
