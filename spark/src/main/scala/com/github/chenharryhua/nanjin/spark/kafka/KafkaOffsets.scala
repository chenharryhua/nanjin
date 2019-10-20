package com.github.chenharryhua.nanjin.spark.kafka

import com.github.chenharryhua.nanjin.kafka.{GenericTopicPartition, KafkaOffsetRange}
import org.apache.spark.streaming.kafka010.OffsetRange

private[kafka] object KafkaOffsets {

  def offsetRange(range: GenericTopicPartition[KafkaOffsetRange]): Array[OffsetRange] =
    range.value.toArray.map {
      case (tp, r) => OffsetRange.create(tp, r.from.value, r.until.value)
    }

  def offsetOptions(range: GenericTopicPartition[KafkaOffsetRange]): Map[String, String] = {
    def poJson(partition: Int, offset: Long)        = s""" "$partition":$offset """
    def osJson(topicName: String, po: List[String]) = s"""{"$topicName":{${po.mkString(",")}}}"""

    val start = range.value.map {
      case (k, v) => poJson(k.partition(), v.from.value)
    }.toList

    val end = range.value.map {
      case (k, v) => poJson(k.partition(), v.until.value)
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
