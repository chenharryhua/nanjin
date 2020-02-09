package com.github.chenharryhua.nanjin.kafka.common

import cats.Bifunctor
import fs2.kafka.{ProducerRecord => Fs2ProducerRecord}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder => JsonDecoder, Encoder => JsonEncoder}
import monocle.macros.Lenses
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord

/**
  * for kafka data persistence
  */
@Lenses final case class NJConsumerRecord[K, V](
  partition: Int,
  offset: Long,
  timestamp: Long,
  key: Option[K],
  value: Option[V],
  topic: String,
  timestampType: Int) {

  def toNJProducerRecord: NJProducerRecord[K, V] =
    NJProducerRecord[K, V](Option(partition), Option(timestamp), key, value)
}

object NJConsumerRecord {

  def apply[K, V](cr: ConsumerRecord[Option[K], Option[V]]): NJConsumerRecord[K, V] =
    NJConsumerRecord(
      cr.partition,
      cr.offset,
      cr.timestamp,
      cr.key,
      cr.value,
      cr.topic,
      cr.timestampType.id)

  implicit def jsonNJConsumerRecordEncoder[K: JsonEncoder, V: JsonEncoder]
    : JsonEncoder[NJConsumerRecord[K, V]] = deriveEncoder[NJConsumerRecord[K, V]]

  implicit def jsonNJConsumerRecordDecoder[K: JsonDecoder, V: JsonDecoder]
    : JsonDecoder[NJConsumerRecord[K, V]] = deriveDecoder[NJConsumerRecord[K, V]]

  implicit val njConsumerRecordBifunctor: Bifunctor[NJConsumerRecord] =
    new Bifunctor[NJConsumerRecord] {

      override def bimap[A, B, C, D](
        fab: NJConsumerRecord[A, B])(f: A => C, g: B => D): NJConsumerRecord[C, D] =
        fab.copy(key = fab.key.map(f), value = fab.value.map(g))
    }
}

@Lenses final case class NJProducerRecord[K, V](
  partition: Option[Int],
  timestamp: Option[Long],
  key: Option[K],
  value: Option[V]) {

  def withPartition(pt: Int): NJProducerRecord[K, V] =
    NJProducerRecord.partition.set(Some(pt))(this)

  def withoutPartition: NJProducerRecord[K, V] =
    NJProducerRecord.partition.set(None)(this)

  def withTimestamp(ts: Long): NJProducerRecord[K, V] =
    NJProducerRecord.timestamp.set(Some(ts))(this)

  def withoutTimestamp: NJProducerRecord[K, V] =
    NJProducerRecord.timestamp.set(None)(this)

  def withoutPartitionAndTimestamp: NJProducerRecord[K, V] =
    NJProducerRecord
      .timestamp[K, V]
      .set(None)
      .andThen(NJProducerRecord.partition[K, V].set(None))(this)

  def withNewKey(k: K): NJProducerRecord[K, V] =
    NJProducerRecord.key.set(Some(k))(this)

  def withNewValue(v: V): NJProducerRecord[K, V] =
    NJProducerRecord.value.set(Some(v))(this)

  @SuppressWarnings(Array("AsInstanceOf"))
  def toFs2ProducerRecord(topicName: TopicName): Fs2ProducerRecord[K, V] = {
    val pr = Fs2ProducerRecord(
      topicName.value,
      key.getOrElse(null.asInstanceOf[K]),
      value.getOrElse(null.asInstanceOf[V]))
    val pt = partition.fold(pr)(pr.withPartition)
    timestamp.fold(pt)(pt.withTimestamp)
  }
}

object NJProducerRecord {

  def apply[K, V](pr: ProducerRecord[Option[K], Option[V]]): NJProducerRecord[K, V] =
    NJProducerRecord(Option(pr.partition), Option(pr.timestamp), pr.key, pr.value)

  def apply[K, V](k: K, v: V): NJProducerRecord[K, V] =
    NJProducerRecord(None, None, Some(k), Some(v))

  def apply[K, V](v: V): NJProducerRecord[K, V] =
    NJProducerRecord(None, None, None, Some(v))

  implicit def jsonNJProducerRecordEncoder[K: JsonEncoder, V: JsonEncoder]
    : JsonEncoder[NJProducerRecord[K, V]] =
    deriveEncoder[NJProducerRecord[K, V]]

  implicit def jsonNJProducerRecordDecoder[K: JsonDecoder, V: JsonDecoder]
    : JsonDecoder[NJProducerRecord[K, V]] =
    deriveDecoder[NJProducerRecord[K, V]]

  implicit val njProducerRecordBifunctor: Bifunctor[NJProducerRecord] =
    new Bifunctor[NJProducerRecord] {

      override def bimap[A, B, C, D](
        fab: NJProducerRecord[A, B])(f: A => C, g: B => D): NJProducerRecord[C, D] =
        fab.copy(key = fab.key.map(f), value = fab.value.map(g))
    }
}
