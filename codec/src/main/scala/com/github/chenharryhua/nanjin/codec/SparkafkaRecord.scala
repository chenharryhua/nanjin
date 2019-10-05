package com.github.chenharryhua.nanjin.codec

import cats.Bifunctor
import monocle.macros.Lenses
import org.apache.kafka.clients.consumer.ConsumerRecord
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

  def flattenKeyValue[K2, V2](
    implicit ev: K <:< Option[K2],
    ev2: V <:< Option[V2]): SparkafkaConsumerRecord[K2, V2] =
    SparkafkaConsumerRecord(
      key.flatten[K2],
      value.flatten[V2],
      topic,
      partition,
      offset,
      timestamp,
      timestampType)

  def toSparkafkaProducerRecord: SparkafkaProducerRecord[K, V] =
    SparkafkaProducerRecord[K, V](topic, Option(partition), Option(timestamp), key, value)
}

object SparkafkaConsumerRecord {

  def fromConsumerRecord[K, V](cr: ConsumerRecord[K, V]): SparkafkaConsumerRecord[K, V] =
    SparkafkaConsumerRecord[K, V](
      Option(cr.key()),
      Option(cr.value()),
      cr.topic(),
      cr.partition(),
      cr.offset(),
      cr.timestamp(),
      cr.timestampType().id)

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

  implicit val SparkafkaProducerRecordBifunctor: Bifunctor[SparkafkaProducerRecord] =
    new Bifunctor[SparkafkaProducerRecord] {

      override def bimap[A, B, C, D](
        fab: SparkafkaProducerRecord[A, B])(f: A => C, g: B => D): SparkafkaProducerRecord[C, D] =
        fab.copy(key = fab.key.map(f), value = fab.value.map(g))
    }

  def apply[K, V](topic: String, k: Option[K], v: Option[V]): SparkafkaProducerRecord[K, V] =
    SparkafkaProducerRecord(topic, None, None, k, v)

  def apply[K, V](topic: String, k: K, v: V): SparkafkaProducerRecord[K, V] =
    SparkafkaProducerRecord(topic, None, None, Option(k), Option(v))

  def apply[K, V](topic: String, v: V): SparkafkaProducerRecord[K, V] =
    SparkafkaProducerRecord(topic, None, None, None, Option(v))

}
