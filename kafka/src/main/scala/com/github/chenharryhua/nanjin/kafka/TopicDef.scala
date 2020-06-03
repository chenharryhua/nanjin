package com.github.chenharryhua.nanjin.kafka

import java.io.ByteArrayOutputStream

import cats.implicits._
import cats.kernel.Eq
import com.github.chenharryhua.nanjin.kafka.codec._
import com.github.chenharryhua.nanjin.kafka.common.NJConsumerRecord
import com.sksamuel.avro4s.{
  AvroInputStream,
  AvroOutputStream,
  SchemaFor,
  Decoder => AvroDecoder,
  Encoder => AvroEncoder
}
import io.circe.Json
import io.circe.parser.parse

import scala.util.Try

final class TopicDef[K, V] private (val topicName: TopicName)(implicit
  val serdeOfKey: SerdeOf[K],
  val serdeOfVal: SerdeOf[V])
    extends Serializable {

  override def toString: String = topicName.value

  val keySchemaLoc: String = s"${topicName.value}-key"
  val valSchemaLoc: String = s"${topicName.value}-value"

  def withTopicName(tn: String): TopicDef[K, V] = TopicDef[K, V](TopicName.unsafeFrom(tn))

  implicit val avroKeyEncoder: AvroEncoder[K] = serdeOfKey.avroEncoder
  implicit val avroKeyDecoder: AvroDecoder[K] = serdeOfKey.avroDecoder

  implicit val avroValEncoder: AvroEncoder[V] = serdeOfVal.avroEncoder
  implicit val avroValDecoder: AvroDecoder[V] = serdeOfVal.avroDecoder

  implicit val keySchemaFor: SchemaFor[K] = serdeOfKey.schemaFor
  implicit val valSchemaFor: SchemaFor[V] = serdeOfVal.schemaFor

  val schemaFor: SchemaFor[NJConsumerRecord[K, V]] = SchemaFor[NJConsumerRecord[K, V]]

  @throws[Exception]
  def toJackson(cr: NJConsumerRecord[K, V]): Json = {
    val byteArrayOutputStream = new ByteArrayOutputStream
    val out =
      AvroOutputStream.json[NJConsumerRecord[K, V]].to(byteArrayOutputStream).build
    out.write(cr)
    out.close()
    parse(byteArrayOutputStream.toString).fold(throw _, identity)
  }

  def fromJackson(cr: String): Try[NJConsumerRecord[K, V]] =
    Try(
      AvroInputStream
        .json[NJConsumerRecord[K, V]]
        .from(cr.getBytes)
        .build(schemaFor.schema)
        .tryIterator
        .next).flatten

  def in[F[_]](ctx: KafkaContext[F]): KafkaTopic[F, K, V] =
    ctx.topic[K, V](this)
}

object TopicDef {

  implicit def eqTopicDef[K, V]: Eq[TopicDef[K, V]] =
    (x: TopicDef[K, V], y: TopicDef[K, V]) =>
      x.topicName.value === y.topicName.value && x.schemaFor == y.schemaFor

  def apply[K, V](
    topicName: TopicName,
    keySchema: WithAvroSchema[K],
    valueSchema: WithAvroSchema[V]): TopicDef[K, V] =
    new TopicDef(topicName)(SerdeOf(keySchema), SerdeOf(valueSchema))

  def apply[K: SerdeOf, V: SerdeOf](topicName: TopicName): TopicDef[K, V] =
    new TopicDef(topicName)(SerdeOf[K], SerdeOf[V])

  def apply[K: SerdeOf, V](topicName: TopicName, valueSchema: WithAvroSchema[V]): TopicDef[K, V] =
    new TopicDef(topicName)(SerdeOf[K], SerdeOf(valueSchema))
}
