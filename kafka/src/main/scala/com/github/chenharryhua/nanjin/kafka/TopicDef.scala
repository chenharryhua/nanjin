package com.github.chenharryhua.nanjin.kafka

import cats.implicits._
import cats.kernel.Eq
import com.github.chenharryhua.nanjin.kafka.codec._
import com.github.chenharryhua.nanjin.kafka.common.{NJConsumerRecord, TopicName}
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

final class TopicDef[K, V] private (val topicName: TopicName)(
  implicit
  val serdeOfKey: SerdeOf[K],
  val serdeOfValue: SerdeOf[V],
  val jsonKeyEncoder: JsonEncoder[K],
  val jsonKeyDecoder: JsonDecoder[K],
  val jsonValueEncoder: JsonEncoder[V],
  val jsonValueDecoder: JsonDecoder[V])
    extends Serializable {
  val keySchemaLoc: String   = s"${topicName.value}-key"
  val valueSchemaLoc: String = s"${topicName.value}-value"

  implicit private val avroKeyEncoder: AvroEncoder[K]   = serdeOfKey.avroEncoder
  implicit private val avroKeyDecoder: AvroDecoder[K]   = serdeOfKey.avroDecoder
  implicit private val avroValueEncoder: AvroEncoder[V] = serdeOfValue.avroEncoder
  implicit private val avroValueDecoder: AvroDecoder[V] = serdeOfValue.avroDecoder

  implicit private val keySchemaFor: SchemaFor[K] =
    (_: FieldMapper) => serdeOfKey.schema

  implicit private val valueSchemaFor: SchemaFor[V] =
    (_: FieldMapper) => serdeOfValue.schema

  val njConsumerRecordSchema: Schema = AvroSchema[NJConsumerRecord[K, V]]

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
}

object TopicDef {

  implicit def eqTopicDef[K, V]: Eq[TopicDef[K, V]] =
    (x: TopicDef[K, V], y: TopicDef[K, V]) =>
      x.topicName === y.topicName && x.njConsumerRecordSchema == y.njConsumerRecordSchema

  def apply[K: JsonEncoder: JsonDecoder, V: JsonEncoder: JsonDecoder](
    topicName: String,
    keySchema: ManualAvroSchema[K],
    valueSchema: ManualAvroSchema[V]): TopicDef[K, V] =
    new TopicDef(TopicName(topicName))(
      SerdeOf(keySchema),
      SerdeOf(valueSchema),
      JsonEncoder[K],
      JsonDecoder[K],
      JsonEncoder[V],
      JsonDecoder[V])

  def apply[K: JsonEncoder: JsonDecoder: SerdeOf, V: JsonEncoder: JsonDecoder: SerdeOf](
    topicName: String): TopicDef[K, V] =
    new TopicDef(TopicName(topicName))(
      SerdeOf[K],
      SerdeOf[V],
      JsonEncoder[K],
      JsonDecoder[K],
      JsonEncoder[V],
      JsonDecoder[V])

  def apply[K: JsonEncoder: JsonDecoder: SerdeOf, V: JsonEncoder: JsonDecoder](
    topicName: String,
    valueSchema: ManualAvroSchema[V]): TopicDef[K, V] =
    new TopicDef(TopicName(topicName))(
      SerdeOf[K],
      SerdeOf(valueSchema),
      JsonEncoder[K],
      JsonDecoder[K],
      JsonEncoder[V],
      JsonDecoder[V])

  def apply[K: JsonEncoder: JsonDecoder, V: JsonEncoder: JsonDecoder: SerdeOf](
    topicName: String,
    keySchema: ManualAvroSchema[K]): TopicDef[K, V] =
    new TopicDef(TopicName(topicName))(
      SerdeOf(keySchema),
      SerdeOf[V],
      JsonEncoder[K],
      JsonDecoder[K],
      JsonEncoder[V],
      JsonDecoder[V])
}
