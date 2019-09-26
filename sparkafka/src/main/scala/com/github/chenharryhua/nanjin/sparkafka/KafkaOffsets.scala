package com.github.chenharryhua.nanjin.sparkafka

import com.github.chenharryhua.nanjin.kafka.{GenericTopicPartition, KafkaOffsetRange}
import org.apache.spark.streaming.kafka010.OffsetRange

private[sparkafka] object KafkaOffsets {

  def offsetRange(range: GenericTopicPartition[KafkaOffsetRange]): Array[OffsetRange] =
    range.value.toArray.map {
      case (tp, r) => OffsetRange.create(tp, r.fromOffset, r.untilOffset)
    }

  def offsetOptions(range: GenericTopicPartition[KafkaOffsetRange]): Map[String, String] = {
    def poJson(partition: Int, offset: Long)        = s""" "$partition":$offset """
    def osJson(topicName: String, po: List[String]) = s"""{"$topicName":{${po.mkString(",")}}}"""

    val start = range.value.map {
      case (k, v) => poJson(k.partition(), v.fromOffset)
    }.toList

    val end = range.value.map {
      case (k, v) => poJson(k.partition(), v.untilOffset)
    }.toList

    range.value.keys.headOption match {
      case Some(t) =>
        Map(
          "startingOffsets" -> osJson(t.topic(), start),
          "endingOffsets" -> osJson(t.topic(), end))
      case None => Map.empty
    }
  }
}
