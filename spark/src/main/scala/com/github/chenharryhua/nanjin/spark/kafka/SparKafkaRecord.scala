package com.github.chenharryhua.nanjin.spark.kafka

import cats.Bifunctor
import com.github.chenharryhua.nanjin.datetime.NJTimestamp
import monocle.macros.Lenses
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import java.time.Clock

// https://spark.apache.org/docs/2.4.3/structured-streaming-kafka-integration.html
@Lenses final case class SparKafkaConsumerRecord[K, V](
  key: Option[K],
  value: Option[V],
  topic: String,
  partition: Int,
  offset: Long,
  timestamp: Long,
  timestampType: Int) {

  def flattenKeyValue[K2, V2](
    implicit ev: K <:< Option[K2],
    ev2: V <:< Option[V2]): SparKafkaConsumerRecord[K2, V2] =
    SparKafkaConsumerRecord(
      key.flatten[K2],
      value.flatten[V2],
      topic,
      partition,
      offset,
      timestamp,
      timestampType)

  def toSparkafkaProducerRecord: SparKafkaProducerRecord[K, V] =
    SparKafkaProducerRecord[K, V](topic, Option(partition), Option(timestamp), key, value)
}

object SparKafkaConsumerRecord {

  def fromConsumerRecord[K, V](cr: ConsumerRecord[K, V]): SparKafkaConsumerRecord[K, V] =
    SparKafkaConsumerRecord[K, V](
      Option(cr.key()),
      Option(cr.value()),
      cr.topic(),
      cr.partition(),
      cr.offset(),
      cr.timestamp(),
      cr.timestampType().id)

  implicit val SparkafkaConsumerRecordBifunctor: Bifunctor[SparKafkaConsumerRecord] =
    new Bifunctor[SparKafkaConsumerRecord] {

      override def bimap[A, B, C, D](
        fab: SparKafkaConsumerRecord[A, B])(f: A => C, g: B => D): SparKafkaConsumerRecord[C, D] =
        fab.copy(key = fab.key.map(f), value = fab.value.map(g))
    }
}

@Lenses final case class SparKafkaProducerRecord[K, V](
  topic: String,
  partition: Option[Int],
  timestamp: Option[Long],
  key: Option[K],
  value: Option[V]) {

  def withTimestamp(ts: Long): SparKafkaProducerRecord[K, V] = copy(timestamp = Some(ts))
  def withPartition(pt: Int): SparKafkaProducerRecord[K, V]  = copy(partition = Some(pt))
  def withoutPartition: SparKafkaProducerRecord[K, V]        = copy(partition = None)

  def withNow(clock: Clock): SparKafkaProducerRecord[K, V] =
    withTimestamp(NJTimestamp.now(clock).milliseconds)

  @SuppressWarnings(Array("AsInstanceOf"))
  def toProducerRecord: ProducerRecord[K, V] = new ProducerRecord[K, V](
    topic,
    partition.getOrElse(null.asInstanceOf[Int]),
    timestamp.getOrElse(null.asInstanceOf[Long]),
    key.getOrElse(null.asInstanceOf[K]),
    value.getOrElse(null.asInstanceOf[V])
  )
}

object SparKafkaProducerRecord {

  implicit val SparkafkaProducerRecordBifunctor: Bifunctor[SparKafkaProducerRecord] =
    new Bifunctor[SparKafkaProducerRecord] {

      override def bimap[A, B, C, D](
        fab: SparKafkaProducerRecord[A, B])(f: A => C, g: B => D): SparKafkaProducerRecord[C, D] =
        fab.copy(key = fab.key.map(f), value = fab.value.map(g))
    }

  def apply[K, V](topic: String, k: Option[K], v: Option[V]): SparKafkaProducerRecord[K, V] =
    SparKafkaProducerRecord(topic, None, None, k, v)

  def apply[K, V](topic: String, k: K, v: V): SparKafkaProducerRecord[K, V] =
    SparKafkaProducerRecord(topic, None, None, Option(k), Option(v))

  def apply[K, V](topic: String, v: V): SparKafkaProducerRecord[K, V] =
    SparKafkaProducerRecord(topic, None, None, None, Option(v))

}
