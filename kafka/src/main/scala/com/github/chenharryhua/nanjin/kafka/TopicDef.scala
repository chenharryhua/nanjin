package com.github.chenharryhua.nanjin.kafka

import java.io.ByteArrayOutputStream

import cats.implicits._
import cats.kernel.Eq
import com.github.chenharryhua.nanjin.kafka.codec._
import com.github.chenharryhua.nanjin.kafka.common.{NJConsumerRecord, TopicName}
import com.sksamuel.avro4s.{
  AvroInputStream,
  AvroOutputStream,
  AvroSchema,
  FieldMapper,
  FromRecord,
  SchemaFor,
  ToRecord,
  Decoder => AvroDecoder,
  Encoder => AvroEncoder
}
import io.circe.Json
import io.circe.parser.parse
import org.apache.avro.Schema

import scala.util.Try

final class TopicDef[K, V] private (val topicName: TopicName)(
  implicit
  val serdeOfKey: SerdeOf[K],
  val serdeOfVal: SerdeOf[V])
    extends Serializable {
  val keySchemaLoc: String = s"${topicName.value}-key"
  val valSchemaLoc: String = s"${topicName.value}-value"

  implicit val avroKeyEncoder: AvroEncoder[K] = serdeOfKey.avroEncoder
  implicit val avroKeyDecoder: AvroDecoder[K] = serdeOfKey.avroDecoder
  implicit val avroValEncoder: AvroEncoder[V] = serdeOfVal.avroEncoder
  implicit val avroValDecoder: AvroDecoder[V] = serdeOfVal.avroDecoder

  implicit val schemaForKey: SchemaFor[K] =
    (_: FieldMapper) => serdeOfKey.schema

  implicit val schemaForVal: SchemaFor[V] =
    (_: FieldMapper) => serdeOfVal.schema

  val njSchema: Schema = AvroSchema[NJConsumerRecord[K, V]]

  val toAvroRecord: ToRecord[NJConsumerRecord[K, V]] =
    ToRecord[NJConsumerRecord[K, V]](njSchema)

  val fromAvroRecord: FromRecord[NJConsumerRecord[K, V]] =
    FromRecord[NJConsumerRecord[K, V]](njSchema)

  @throws[Exception]
  def toJackson(cr: NJConsumerRecord[K, V]): Json = {
    val byteArrayOutputStream = new ByteArrayOutputStream
    val out =
      AvroOutputStream.json[NJConsumerRecord[K, V]].to(byteArrayOutputStream).build(njSchema)
    out.write(cr)
    out.close()
    parse(byteArrayOutputStream.toString).fold(throw _, identity)
  }

  def fromJackson(cr: String): Try[NJConsumerRecord[K, V]] =
    Try(
      AvroInputStream
        .json[NJConsumerRecord[K, V]]
        .from(cr.getBytes)
        .build(njSchema)
        .tryIterator
        .next).flatten

  def in[F[_]](ctx: KafkaContext[F]): KafkaTopic[F, K, V] =
    ctx.topic[K, V](this)
}

object TopicDef {

  implicit def eqTopicDef[K, V]: Eq[TopicDef[K, V]] =
    (x: TopicDef[K, V], y: TopicDef[K, V]) =>
      x.topicName === y.topicName && x.njSchema == y.njSchema

  def apply[K, V](
    topicName: String,
    keySchema: ManualAvroSchema[K],
    valueSchema: ManualAvroSchema[V]): TopicDef[K, V] =
    new TopicDef(TopicName(topicName))(SerdeOf(keySchema), SerdeOf(valueSchema))

  def apply[K: SerdeOf, V: SerdeOf](topicName: String): TopicDef[K, V] =
    new TopicDef(TopicName(topicName))(SerdeOf[K], SerdeOf[V])

  def apply[K: SerdeOf, V](topicName: String, valueSchema: ManualAvroSchema[V]): TopicDef[K, V] =
    new TopicDef(TopicName(topicName))(SerdeOf[K], SerdeOf(valueSchema))
}
