package com.github.chenharryhua.nanjin.spark.kafka

import com.github.chenharryhua.nanjin.kafka.{GenericTopicPartition, KafkaOffsetRange}
import org.apache.spark.streaming.kafka010.OffsetRange

private[kafka] object KafkaOffsets {

  def offsetRange(range: GenericTopicPartition[KafkaOffsetRange]): Array[OffsetRange] =
    range.value.toArray.map {
      case (tp, r) => OffsetRange.create(tp, r.from.value, r.until.value)
    }

}
