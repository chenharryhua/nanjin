package com.github.chenharryhua.nanjin.kafka

import java.io.{ByteArrayOutputStream, OutputStream}

import cats.effect.{Resource, Sync}
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
import fs2.{Pipe, Stream}
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

  val njConsumerRecordSchema: Schema = AvroSchema[NJConsumerRecord[K, V]]

  private val toAvroRecord: ToRecord[NJConsumerRecord[K, V]] =
    ToRecord[NJConsumerRecord[K, V]](njConsumerRecordSchema)

  private val fromAvroRecord: FromRecord[NJConsumerRecord[K, V]] =
    FromRecord[NJConsumerRecord[K, V]](njConsumerRecordSchema)

  def toAvro(cr: NJConsumerRecord[K, V]): Record   = toAvroRecord.to(cr)
  def fromAvro(cr: Record): NJConsumerRecord[K, V] = fromAvroRecord.from(cr)

  def avroSink[F[_]: Sync](os: OutputStream): Pipe[F, NJConsumerRecord[K, V], Unit] = {
    val avro = Resource.make(
      Sync[F].pure(
        AvroOutputStream.data[NJConsumerRecord[K, V]].to(os).build(njConsumerRecordSchema)))(aos =>
      Sync[F].delay(aos.close))
    in => Stream.resource(avro).flatMap(o => in.map(o.write))
  }

  def jacksonSink[F[_]: Sync](os: OutputStream): Pipe[F, NJConsumerRecord[K, V], Unit] = {

    val json = Resource.make(
      Sync[F].pure(
        AvroOutputStream.json[NJConsumerRecord[K, V]].to(os).build(njConsumerRecordSchema)))(aos =>
      Sync[F].delay(aos.close))
    in => Stream.resource(json).flatMap(o => in.map(o.write))
  }

  def valueAvroSink[F[_]: Sync](os: OutputStream): Pipe[F, V, Unit] = {
    val avro =
      Resource.make(Sync[F].pure(AvroOutputStream.data[V].to(os).build(serdeOfVal.schema)))(aos =>
        Sync[F].delay(aos.close))
    in => Stream.resource(avro).flatMap(o => in.map(o.write))
  }

  def valueJacksonSink[F[_]: Sync](os: OutputStream): Pipe[F, V, Unit] = {
    val json =
      Resource.make(Sync[F].pure(AvroOutputStream.json[V].to(os).build(serdeOfVal.schema)))(aos =>
        Sync[F].delay(aos.close))
    in => Stream.resource(json).flatMap(o => in.map(o.write))
  }

  @throws[Exception]
  def toJackson(cr: NJConsumerRecord[K, V]): Json = {
    val byteArrayOutputStream = new ByteArrayOutputStream
    val out =
      AvroOutputStream
        .json[NJConsumerRecord[K, V]]
        .to(byteArrayOutputStream)
        .build(njConsumerRecordSchema)
    out.write(cr)
    out.close()
    parse(byteArrayOutputStream.toString).fold(throw _, identity)
  }

  def fromJackson(cr: String): Try[NJConsumerRecord[K, V]] =
    Try(
      AvroInputStream
        .json[NJConsumerRecord[K, V]]
        .from(cr.getBytes)
        .build(njConsumerRecordSchema)
        .tryIterator
        .next).flatten

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
