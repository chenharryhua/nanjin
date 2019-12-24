package com.github.chenharryhua.nanjin.codec

import com.sksamuel.avro4s.{Record, SchemaFor, ToRecord, Encoder => AvroEncoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder => JsonDecoder, Encoder => JsonEncoder}
import org.apache.kafka.clients.consumer.ConsumerRecord

final case class NJConsumerRecord[K, V](
  key: Option[K],
  value: Option[V],
  topic: String,
  partition: Int,
  offset: Long,
  timestamp: Long,
  timestampType: String) {

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
      Option(cr.key),
      Option(cr.value),
      cr.topic,
      cr.partition,
      cr.offset,
      cr.timestamp,
      cr.timestampType.toString)

  implicit def jsonNJConsumerRecordEncoder[K: JsonEncoder, V: JsonEncoder]
    : JsonEncoder[NJConsumerRecord[K, V]] =
    deriveEncoder[NJConsumerRecord[K, V]]

  implicit def jsonNJConsumerRecordDecoder[K: JsonDecoder, V: JsonDecoder]
    : JsonDecoder[NJConsumerRecord[K, V]] =
    deriveDecoder[NJConsumerRecord[K, V]]
}
