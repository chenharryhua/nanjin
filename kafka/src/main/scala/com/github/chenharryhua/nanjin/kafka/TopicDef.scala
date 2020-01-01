package com.github.chenharryhua.nanjin.kafka

import cats.Show
import com.github.chenharryhua.nanjin.kafka.codec.SerdeOf
import com.sksamuel.avro4s.{
  AvroSchema,
  FromRecord,
  Record,
  SchemaFor,
  ToRecord,
  Decoder => AvroDecoder,
  Encoder => AvroEncoder
}
import io.circe.parser.decode
import io.circe.syntax._
import io.circe.{Error, Json, Decoder => JsonDecoder, Encoder => JsonEncoder}
import org.apache.avro.Schema

final class TopicDef[K, V] private (val topicName: String)(
  implicit
  val serdeOfKey: SerdeOf[K],
  val serdeOfValue: SerdeOf[V],
  val showKey: Show[K],
  val showValue: Show[V],
  jsonKeyEncoder: JsonEncoder[K],
  jsonValueEncoder: JsonEncoder[V],
  jsonKeyDecoder: JsonDecoder[K],
  jsonValueDecoder: JsonDecoder[V],
  avroKeyEncoder: AvroEncoder[K],
  avroValueEncoder: AvroEncoder[V],
  avroKeyDecoder: AvroDecoder[K],
  avroValueDecoder: AvroDecoder[V]
) {
  val keySchemaLoc: String   = s"$topicName-key"
  val valueSchemaLoc: String = s"$topicName-value"

  implicit private val keySchemaFor: SchemaFor[K]   = SchemaFor.const(serdeOfKey.schema)
  implicit private val valueSchemaFor: SchemaFor[V] = SchemaFor.const(serdeOfValue.schema)
  val njConsumerRecordSchema: Schema                = AvroSchema[NJConsumerRecord[K, V]]

  private val toAvroRecord: ToRecord[NJConsumerRecord[K, V]] =
    ToRecord[NJConsumerRecord[K, V]](njConsumerRecordSchema)

  private val fromAvroRecord: FromRecord[NJConsumerRecord[K, V]] =
    FromRecord[NJConsumerRecord[K, V]](njConsumerRecordSchema)

  def toAvro(cr: NJConsumerRecord[K, V]): Record   = toAvroRecord.to(cr)
  def fromAvro(cr: Record): NJConsumerRecord[K, V] = fromAvroRecord.from(cr)

  def toJson(cr: NJConsumerRecord[K, V]): Json = cr.asJson

  def fromJson(cr: String): Either[Error, NJConsumerRecord[K, V]] =
    decode[NJConsumerRecord[K, V]](cr)

  def in[F[_]](ctx: KafkaContext[F]): KafkaTopic[F, K, V] =
    ctx.topic[K, V](this)

  def show: String = s"TopicDef(topicName = ${topicName})"
}

object TopicDef {
  implicit def showTopicDef[K, V]: Show[TopicDef[K, V]] = _.show

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
