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
  Record,
  SchemaFor,
  ToRecord,
  Decoder => AvroDecoder,
  Encoder => AvroEncoder
}
import org.apache.avro.Schema

import scala.util.Try

final class TopicDef[K, V] private (val topicName: TopicName)(
  implicit
  val serdeOfKey: SerdeOf[K],
  val serdeOfValue: SerdeOf[V])
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

  def toJson(cr: NJConsumerRecord[K, V]): String = {
    val byteArrayOutputStream = new ByteArrayOutputStream
    val out =
      AvroOutputStream
        .json[NJConsumerRecord[K, V]]
        .to(byteArrayOutputStream)
        .build(njConsumerRecordSchema)
    out.write(cr)
    out.close()
    byteArrayOutputStream.toString
  }

  def fromJson(cr: String): Try[NJConsumerRecord[K, V]] =
    AvroInputStream
      .json[NJConsumerRecord[K, V]]
      .from(cr.getBytes)
      .build(njConsumerRecordSchema)
      .tryIterator
      .next

  def in[F[_]](ctx: KafkaContext[F]): KafkaTopic[F, K, V] =
    ctx.topic[K, V](this)
}

object TopicDef {

  implicit def eqTopicDef[K, V]: Eq[TopicDef[K, V]] =
    (x: TopicDef[K, V], y: TopicDef[K, V]) =>
      x.topicName === y.topicName && x.njConsumerRecordSchema == y.njConsumerRecordSchema

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
