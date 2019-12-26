package com.github.chenharryhua.nanjin.codec

import java.time.Clock

import cats.Bifunctor
import com.github.chenharryhua.nanjin.datetime.NJTimestamp
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder => JsonDecoder, Encoder => JsonEncoder}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord

/**
  * for kafka data persistence
  */
final case class NJConsumerRecord[K, V](
  partition: Int,
  offset: Long,
  timestamp: Long,
  key: Option[K],
  value: Option[V],
  topic: String,
  timestampType: Int) {

  def toNJProducerRecord: NJProducerRecord[K, V] =
    NJProducerRecord[K, V](topic, Option(partition), Option(timestamp), key, value)

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
    : JsonEncoder[NJConsumerRecord[K, V]] =
    deriveEncoder[NJConsumerRecord[K, V]]

  implicit def jsonNJConsumerRecordDecoder[K: JsonDecoder, V: JsonDecoder]
    : JsonDecoder[NJConsumerRecord[K, V]] =
    deriveDecoder[NJConsumerRecord[K, V]]

  implicit val njConsumerRecordBifunctor: Bifunctor[NJConsumerRecord] =
    new Bifunctor[NJConsumerRecord] {

      override def bimap[A, B, C, D](
        fab: NJConsumerRecord[A, B])(f: A => C, g: B => D): NJConsumerRecord[C, D] =
        fab.copy(key = fab.key.map(f), value = fab.value.map(g))
    }
}

final case class NJProducerRecord[K, V](
  topic: String,
  partition: Option[Int],
  timestamp: Option[Long],
  key: Option[K],
  value: Option[V]) {

  def withTimestamp(ts: Long): NJProducerRecord[K, V] = copy(timestamp = Some(ts))
  def withPartition(pt: Int): NJProducerRecord[K, V]  = copy(partition = Some(pt))
  def withoutPartition: NJProducerRecord[K, V]        = copy(partition = None)
  def withTopic(tpk: String): NJProducerRecord[K, V]  = copy(topic     = tpk)

  def withNow(clock: Clock): NJProducerRecord[K, V] =
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

object NJProducerRecord {

  def apply[K, V](pr: ProducerRecord[Option[K], Option[V]]): NJProducerRecord[K, V] =
    NJProducerRecord(pr.topic, Option(pr.partition), Option(pr.timestamp), pr.key, pr.value)

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
