package com.github.chenharryhua.nanjin.kafka

import cats.Show
import monocle.macros.Lenses
import org.apache.kafka.clients.consumer.ConsumerRecord

@Lenses final case class SparkConsumerRecord[K, V](
  topic: String,
  partition: Int,
  offset: Long,
  key: K,
  value: V,
  timestamp: Long,
  timestampType: String,
  serializedKeySize: Int,
  serializedValueSize: Int,
  headers: String,
  leaderEpoch: String)

object SparkConsumerRecord {

  def from[K, V](d: ConsumerRecord[K, V]): SparkConsumerRecord[K, V] =
    SparkConsumerRecord(
      d.topic(),
      d.partition(),
      d.offset(),
      d.key(),
      d.value(),
      d.timestamp(),
      d.timestampType().name,
      d.serializedKeySize(),
      d.serializedValueSize(),
      d.headers().toString,
      d.leaderEpoch().toString
    )
  implicit def showSparkafkaConsumerRecord[K: Show, V: Show]: Show[SparkConsumerRecord[K, V]] =
    (t: SparkConsumerRecord[K, V]) => {
      val (utc, local) = utils.kafkaTimestamp(t.timestamp)
      s"""
         |topic:               ${t.topic}
         |partition:           ${t.partition}
         |offset:              ${t.offset}
         |key:                 $key
         |value:               $value
         |timestamp:           ${t.timestamp}
         |utc:                 $utc
         |local:               $local
         |time-stamp-type:     ${t.timestampType}
         |serializedKeySize:   ${t.serializedKeySize}
         |serializedValueSize: ${t.serializedValueSize}
         |headers:             ${t.headers}
         |leaderEpoch:         ${t.leaderEpoch}
         |""".stripMargin
    }
}
