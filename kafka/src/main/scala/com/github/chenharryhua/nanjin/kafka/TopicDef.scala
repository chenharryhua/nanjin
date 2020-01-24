package com.github.chenharryhua.nanjin.kafka

import cats.Show
import cats.implicits._
import cats.kernel.Eq
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
import eu.timepit.refined.api.Refined
import eu.timepit.refined.refineV
import eu.timepit.refined.string._
import io.circe.parser.decode
import io.circe.syntax._
import io.circe.{Error, Json, Decoder => JsonDecoder, Encoder => JsonEncoder}
import org.apache.avro.Schema
import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import shapeless.Witness

final class TopicName(name: Refined[String, TopicName.Constraint]) extends Serializable {
  val value: String             = name.value
  override val toString: String = value
}

object TopicName {
  type Constraint = MatchesRegex[Witness.`"^[a-zA-Z0-9_.-]+$"`.T]

  implicit val eqshowTopicName: Eq[TopicName] with Show[TopicName] =
    new Eq[TopicName] with Show[TopicName] {

      override def eqv(x: TopicName, y: TopicName): Boolean =
        x.value === y.value

      override def show(t: TopicName): String =
        t.value
    }

  @throws[Exception]
  def apply(name: String): TopicName =
    refineV[Constraint](name).map(new TopicName(_)).fold(e => throw new Exception(e), identity)
}

final class TopicDef[K, V] private (val topicName: TopicName)(
  implicit
  val serdeOfKey: SerdeOf[K],
  val serdeOfValue: SerdeOf[V],
  val showKey: Show[K],
  val showValue: Show[V],
  val jsonKeyEncoder: JsonEncoder[K],
  val jsonValueEncoder: JsonEncoder[V],
  val jsonKeyDecoder: JsonDecoder[K],
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
}

object TopicDef {

  implicit def eqTopicDef[K, V]: Eq[TopicDef[K, V]] =
    (x: TopicDef[K, V], y: TopicDef[K, V]) =>
      x.topicName === y.topicName && x.njConsumerRecordSchema == y.njConsumerRecordSchema

  def apply[K: Show: JsonEncoder: JsonDecoder, V: Show: JsonEncoder: JsonDecoder](
    topicName: String,
    keySchema: ManualAvroSchema[K],
    valueSchema: ManualAvroSchema[V]): TopicDef[K, V] =
    new TopicDef(TopicName(topicName))(
      SerdeOf(keySchema),
      SerdeOf(valueSchema),
      Show[K],
      Show[V],
      JsonEncoder[K],
      JsonEncoder[V],
      JsonDecoder[K],
      JsonDecoder[V])

  def apply[K: Show: JsonEncoder: JsonDecoder: SerdeOf, V: Show: JsonEncoder: JsonDecoder: SerdeOf](
    topicName: String): TopicDef[K, V] =
    new TopicDef(TopicName(topicName))(
      SerdeOf[K],
      SerdeOf[V],
      Show[K],
      Show[V],
      JsonEncoder[K],
      JsonEncoder[V],
      JsonDecoder[K],
      JsonDecoder[V])

  def apply[K: Show: JsonEncoder: JsonDecoder: SerdeOf, V: Show: JsonEncoder: JsonDecoder](
    topicName: String,
    valueSchema: ManualAvroSchema[V]): TopicDef[K, V] =
    new TopicDef(TopicName(topicName))(
      SerdeOf[K],
      SerdeOf(valueSchema),
      Show[K],
      Show[V],
      JsonEncoder[K],
      JsonEncoder[V],
      JsonDecoder[K],
      JsonDecoder[V])

  def apply[K: Show: JsonEncoder: JsonDecoder, V: Show: JsonEncoder: JsonDecoder: SerdeOf](
    topicName: String,
    keySchema: ManualAvroSchema[K]): TopicDef[K, V] =
    new TopicDef(TopicName(topicName))(
      SerdeOf(keySchema),
      SerdeOf[V],
      Show[K],
      Show[V],
      JsonEncoder[K],
      JsonEncoder[V],
      JsonDecoder[K],
      JsonDecoder[V])
}

final class TopicCodec[K, V] private[kafka] (val keyCodec: NJCodec[K], val valueCodec: NJCodec[V])
    extends Serializable {
  require(
    keyCodec.topicName === valueCodec.topicName,
    "key and value codec should have same topic name")
  val keySerde: NJSerde[K]               = keyCodec.serde
  val valueSerde: NJSerde[V]             = valueCodec.serde
  val keySchema: Schema                  = keySerde.schema
  val valueSchema: Schema                = valueSerde.schema
  val keySerializer: Serializer[K]       = keySerde.serializer
  val keyDeserializer: Deserializer[K]   = keySerde.deserializer
  val valueSerializer: Serializer[V]     = valueSerde.serializer
  val valueDeserializer: Deserializer[V] = valueSerde.deserializer
}
