package com.github.chenharryhua.nanjin.sparkafka

import cats.Show
import com.github.chenharryhua.nanjin.kafka.utils
import monocle.macros.Lenses
import org.apache.kafka.clients.consumer.ConsumerRecord
import cats.implicits._

@Lenses final case class SparkafkaConsumerRecord[K, V](
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

private[sparkafka] trait LowestPriorityShow {

  def build[K, V](t: SparkafkaConsumerRecord[K, V], key: String, value: String): String = {
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

  implicit def showSparkafkaConsumerRecord2[K, V]: Show[SparkafkaConsumerRecord[K, V]] =
    (t: SparkafkaConsumerRecord[K, V]) => build(t, t.key.toString, t.value.toString)
}

private[sparkafka] trait LowPriorityShow extends LowestPriorityShow {
  implicit def showSparkafkaConsumerRecord1[K, V: Show]: Show[SparkafkaConsumerRecord[K, V]] =
    (t: SparkafkaConsumerRecord[K, V]) => build(t, t.key.toString, t.value.show)
}

object SparkafkaConsumerRecord extends LowPriorityShow {

  def from[K, V](d: ConsumerRecord[K, V]): SparkafkaConsumerRecord[K, V] =
    SparkafkaConsumerRecord(
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
  implicit def showSparkafkaConsumerRecord0[K: Show, V: Show]: Show[SparkafkaConsumerRecord[K, V]] =
    (t: SparkafkaConsumerRecord[K, V]) => build(t, t.key.show, t.value.show)
}
