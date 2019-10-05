package com.github.chenharryhua.nanjin.codec

import cats.Bifunctor
import monocle.macros.Lenses
import org.apache.kafka.clients.producer.ProducerRecord

// https://spark.apache.org/docs/2.4.3/structured-streaming-kafka-integration.html
@Lenses final case class SparkafkaConsumerRecord[K, V](
  key: Option[K],
  value: Option[V],
  topic: String,
  partition: Int,
  offset: Long,
  timestamp: Long,
  timestampType: Int) {

  def toSparkafkaProducerRecord: SparkafkaProducerRecord[K, V] =
    SparkafkaProducerRecord[K, V](topic, Option(partition), Option(timestamp), key, value)
}

object SparkafkaConsumerRecord {

  implicit val SparkafkaConsumerRecordBifunctor: Bifunctor[SparkafkaConsumerRecord] =
    new Bifunctor[SparkafkaConsumerRecord] {

      override def bimap[A, B, C, D](
        fab: SparkafkaConsumerRecord[A, B])(f: A => C, g: B => D): SparkafkaConsumerRecord[C, D] =
        fab.copy(key = fab.key.map(f), value = fab.value.map(g))
    }
}

@Lenses final case class SparkafkaProducerRecord[K, V](
  topic: String,
  partition: Option[Int],
  timestamp: Option[Long],
  key: Option[K],
  value: Option[V]) {

  def withTimestamp(ts: Long): SparkafkaProducerRecord[K, V] = copy(timestamp = Some(ts))
  def withPartition(pt: Int): SparkafkaProducerRecord[K, V]  = copy(partition = Some(pt))
  def withoutTimestamp: SparkafkaProducerRecord[K, V]        = copy(timestamp = None)
  def withoutPartition: SparkafkaProducerRecord[K, V]        = copy(partition = None)

  @SuppressWarnings(Array("AsInstanceOf"))
  def toProducerRecord: ProducerRecord[K, V] = new ProducerRecord[K, V](
    topic,
    partition.getOrElse(null.asInstanceOf[Int]),
    timestamp.getOrElse(null.asInstanceOf[Long]),
    key.getOrElse(null.asInstanceOf[K]),
    value.getOrElse(null.asInstanceOf[V])
  )
}

object SparkafkaProducerRecord {

  def apply[K, V](topic: String, k: K, v: V): SparkafkaProducerRecord[K, V] =
    SparkafkaProducerRecord(topic, None, None, Option(k), Option(v))

  def apply[K, V](topic: String, v: V): SparkafkaProducerRecord[K, V] =
    SparkafkaProducerRecord(topic, None, None, None, Option(v))

}
