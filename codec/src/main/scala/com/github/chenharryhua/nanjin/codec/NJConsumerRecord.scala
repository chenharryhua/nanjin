package com.github.chenharryhua.nanjin.codec

import com.sksamuel.avro4s.{Record, SchemaFor, ToRecord, Encoder => AvroEncoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder => JsonDecoder, Encoder => JsonEncoder}
import org.apache.kafka.clients.consumer.ConsumerRecord

/**
  *
  * for kafka data persistence
  *
  * @param partition: kafka partition
  * @param offset: kafka offset
  * @param ts: kafka timestamp
  * @param key: key
  * @param value: value
  * @param topic: kafka topic
  * @param tsType: kafka timestamp type
  * @tparam K: key type
  * @tparam V: value type
  */
final case class NJConsumerRecord[K, V](
  partition: Int,
  offset: Long,
  ts: Long,
  key: Option[K],
  value: Option[V],
  topic: String,
  tsType: String) {

  def asAvro(
    implicit
    ks: SchemaFor[K],
    ke: AvroEncoder[K],
    vs: SchemaFor[V],
    ve: AvroEncoder[V]): Record =
    ToRecord[NJConsumerRecord[K, V]].to(this)
}

object NJConsumerRecord {

  def apply[K, V](cr: ConsumerRecord[K, V]): NJConsumerRecord[K, V] =
    NJConsumerRecord(
      cr.partition,
      cr.offset,
      cr.timestamp,
      Option(cr.key),
      Option(cr.value),
      cr.topic,
      cr.timestampType.toString)

  implicit def jsonNJConsumerRecordEncoder[K: JsonEncoder, V: JsonEncoder]
    : JsonEncoder[NJConsumerRecord[K, V]] =
    deriveEncoder[NJConsumerRecord[K, V]]

  implicit def jsonNJConsumerRecordDecoder[K: JsonDecoder, V: JsonDecoder]
    : JsonDecoder[NJConsumerRecord[K, V]] =
    deriveDecoder[NJConsumerRecord[K, V]]
}
