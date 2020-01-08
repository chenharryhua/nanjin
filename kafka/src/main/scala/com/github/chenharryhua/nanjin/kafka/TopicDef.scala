package com.github.chenharryhua.nanjin.kafka

import cats.derived.auto.eq._
import cats.implicits._
import cats.Show
import com.github.chenharryhua.nanjin.kafka.codec._
import com.sksamuel.avro4s.{
  AvroSchema,
  FieldMapper,
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
import org.apache.kafka.common.serialization.{Deserializer, Serializer}

final case class TopicName(value: String) {
  override val toString: String = value
}

final case class TopicDef[K, V] private (topicName: TopicName)(
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
  avroValueDecoder: AvroDecoder[V]) {
  val keySchemaLoc: String   = s"${topicName.value}-key"
  val valueSchemaLoc: String = s"${topicName.value}-value"

  implicit private val keySchemaFor: SchemaFor[K] =
    (_: FieldMapper) => serdeOfKey.schema

  implicit private val valueSchemaFor: SchemaFor[V] =
    (_: FieldMapper) => serdeOfValue.schema

  val njConsumerRecordSchema: Schema = AvroSchema[NJConsumerRecord[K, V]]

  @transient private lazy val toAvroRecord: ToRecord[NJConsumerRecord[K, V]] =
    ToRecord[NJConsumerRecord[K, V]](njConsumerRecordSchema)

  @transient private lazy val fromAvroRecord: FromRecord[NJConsumerRecord[K, V]] =
    FromRecord[NJConsumerRecord[K, V]](njConsumerRecordSchema)

  def toAvro(cr: NJConsumerRecord[K, V]): Record   = toAvroRecord.to(cr)
  def fromAvro(cr: Record): NJConsumerRecord[K, V] = fromAvroRecord.from(cr)

  def toJson(cr: NJConsumerRecord[K, V]): Json = cr.asJson

  def fromJson(cr: String): Either[Error, NJConsumerRecord[K, V]] =
    decode[NJConsumerRecord[K, V]](cr)

  def in[F[_]](ctx: KafkaContext[F]): KafkaTopic[F, K, V] =
    ctx.topic[K, V](this)

  def show: String = s"TopicDef(topicName = $topicName)"
}

object TopicDef {
  implicit def showTopicDef[K, V]: Show[TopicDef[K, V]] = _.show

  def apply[
    K: Show: JsonEncoder: JsonDecoder: AvroEncoder: AvroDecoder,
    V: Show: JsonEncoder: JsonDecoder: AvroEncoder: AvroDecoder](
    topicName: String,
    keySchema: ManualAvroSchema[K],
    valueSchema: ManualAvroSchema[V]): TopicDef[K, V] =
    TopicDef(TopicName(topicName))(
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
      AvroDecoder[V]
    )

  def apply[
    K: Show: JsonEncoder: JsonDecoder: AvroEncoder: AvroDecoder: SerdeOf,
    V: Show: JsonEncoder: JsonDecoder: AvroEncoder: AvroDecoder: SerdeOf](
    topicName: String): TopicDef[K, V] =
    TopicDef(TopicName(topicName))(
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
      AvroDecoder[V]
    )

  def apply[
    K: Show: JsonEncoder: JsonDecoder: AvroEncoder: AvroDecoder: SerdeOf,
    V: Show: JsonEncoder: JsonDecoder: AvroEncoder: AvroDecoder](
    topicName: String,
    valueSchema: ManualAvroSchema[V]): TopicDef[K, V] =
    TopicDef(TopicName(topicName))(
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
      AvroDecoder[V]
    )

  def apply[
    K: Show: JsonEncoder: JsonDecoder: AvroEncoder: AvroDecoder,
    V: Show: JsonEncoder: JsonDecoder: AvroEncoder: AvroDecoder: SerdeOf](
    topicName: String,
    keySchema: ManualAvroSchema[K]): TopicDef[K, V] =
    TopicDef(TopicName(topicName))(
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
      AvroDecoder[V]
    )
}

final case class TopicCodec[K, V] private[kafka] (
  keyCodec: KafkaCodec.Key[K],
  valueCodec: KafkaCodec.Value[V]) {
  require(
    keyCodec.topicName === valueCodec.topicName,
    "key and value codec should have same topic name")
  val keySerde: KafkaSerde.Key[K]        = keyCodec.serde
  val valueSerde: KafkaSerde.Value[V]    = valueCodec.serde
  val keySchema: Schema                  = keySerde.schema
  val valueSchema: Schema                = valueSerde.schema
  val keySerializer: Serializer[K]       = keySerde.serializer
  val keyDeserializer: Deserializer[K]   = keySerde.deserializer
  val valueSerializer: Serializer[V]     = valueSerde.serializer
  val valueDeserializer: Deserializer[V] = valueSerde.deserializer
}
