package com.github.chenharryhua.nanjin.kafka

import cats.Show
import com.github.chenharryhua.nanjin.codec.{
  ManualAvroSchema,
  NJConsumerRecord,
  NJProducerRecord,
  SerdeOf
}
import com.sksamuel.avro4s.{AvroSchema, SchemaFor, Decoder => AvroDecoder, Encoder => AvroEncoder}
import io.circe.{Decoder                                   => JsonDecoder, Encoder => JsonEncoder}
import org.apache.avro.Schema

final class TopicDef[K, V] private (val topicName: String)(
  implicit
  val serdeOfKey: SerdeOf[K],
  val serdeOfValue: SerdeOf[V],
  val showKey: Show[K],
  val showValue: Show[V],
  val jsonKeyEncoder: JsonEncoder[K],
  val jsonValueEncoder: JsonEncoder[V],
  val jsonKeyDecoder: JsonDecoder[K],
  val jsonValueDecoder: JsonDecoder[V],
  val avroKeyEncoder: AvroEncoder[K],
  val avroValueEncoder: AvroEncoder[V],
  val avroKeyDecoder: AvroDecoder[K],
  val avroValueDecoder: AvroDecoder[V]
) {
  val keySchemaLoc: String   = s"$topicName-key"
  val valueSchemaLoc: String = s"$topicName-value"

  implicit private val keySchemaFor: SchemaFor[K]   = SchemaFor.const(serdeOfKey.schema)
  implicit private val valueSchemaFor: SchemaFor[V] = SchemaFor.const(serdeOfValue.schema)
  val njConsumerRecordSchema: Schema                = AvroSchema[NJConsumerRecord[K, V]]
  val njProducerRecordSchema: Schema                = AvroSchema[NJProducerRecord[K, V]]

  def in[F[_]](ctx: KafkaContext[F]): KafkaTopic[F, K, V] =
    ctx.topic[K, V](this)
}

object TopicDef {

  def apply[
    K: Show: JsonEncoder: JsonDecoder: AvroEncoder: AvroDecoder: SerdeOf,
    V: Show: JsonEncoder: JsonDecoder: AvroEncoder: AvroDecoder: SerdeOf](
    topicName: String): TopicDef[K, V] =
    new TopicDef(topicName)(
      SerdeOf[K],
      SerdeOf[V],
      Show[K],
      Show[V],
      JsonEncoder[K],
      JsonEncoder[V],
      JsonDecoder[K],
      JsonDecoder[V],
      AvroEncoder[K],
      AvroEncoder[V],
      AvroDecoder[K],
      AvroDecoder[V])

  def apply[
    K: Show: JsonEncoder: JsonDecoder: AvroEncoder: AvroDecoder,
    V: Show: JsonEncoder: JsonDecoder: AvroEncoder: AvroDecoder](
    topicName: String,
    keySchema: ManualAvroSchema[K],
    valueSchema: ManualAvroSchema[V]): TopicDef[K, V] =
    new TopicDef(topicName)(
      SerdeOf(keySchema),
      SerdeOf(valueSchema),
      Show[K],
      Show[V],
      JsonEncoder[K],
      JsonEncoder[V],
      JsonDecoder[K],
      JsonDecoder[V],
      AvroEncoder[K],
      AvroEncoder[V],
      AvroDecoder[K],
      AvroDecoder[V])

  def apply[
    K: Show: JsonEncoder: JsonDecoder: AvroEncoder: AvroDecoder: SerdeOf,
    V: Show: JsonEncoder: JsonDecoder: AvroEncoder: AvroDecoder](
    topicName: String,
    valueSchema: ManualAvroSchema[V]): TopicDef[K, V] =
    new TopicDef(topicName)(
      SerdeOf[K],
      SerdeOf(valueSchema),
      Show[K],
      Show[V],
      JsonEncoder[K],
      JsonEncoder[V],
      JsonDecoder[K],
      JsonDecoder[V],
      AvroEncoder[K],
      AvroEncoder[V],
      AvroDecoder[K],
      AvroDecoder[V])

  def apply[
    K: Show: JsonEncoder: JsonDecoder: AvroEncoder: AvroDecoder,
    V: Show: JsonEncoder: JsonDecoder: AvroEncoder: AvroDecoder: SerdeOf](
    topicName: String,
    keySchema: ManualAvroSchema[K]): TopicDef[K, V] =
    new TopicDef(topicName)(
      SerdeOf(keySchema),
      SerdeOf[V],
      Show[K],
      Show[V],
      JsonEncoder[K],
      JsonEncoder[V],
      JsonDecoder[K],
      JsonDecoder[V],
      AvroEncoder[K],
      AvroEncoder[V],
      AvroDecoder[K],
      AvroDecoder[V])
}
