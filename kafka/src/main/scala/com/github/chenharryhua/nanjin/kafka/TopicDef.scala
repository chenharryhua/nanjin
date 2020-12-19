package com.github.chenharryhua.nanjin.kafka

import cats.Show
import cats.kernel.Eq
import cats.syntax.all._
import com.github.chenharryhua.nanjin.messages.kafka.codec.{AvroCodec, SerdeOf}
import com.sksamuel.avro4s.{SchemaFor, Decoder => AvroDecoder, Encoder => AvroEncoder}

final class TopicDef[K, V] private (val topicName: TopicName)(implicit
  val serdeOfKey: SerdeOf[K],
  val serdeOfVal: SerdeOf[V])
    extends Serializable {

  override def toString: String = topicName.value

  def withTopicName(tn: String): TopicDef[K, V] = TopicDef[K, V](TopicName.unsafeFrom(tn))

  val avroKeyEncoder: AvroEncoder[K] = serdeOfKey.avroCodec.avroEncoder
  val avroKeyDecoder: AvroDecoder[K] = serdeOfKey.avroCodec.avroDecoder

  val avroValEncoder: AvroEncoder[V] = serdeOfVal.avroCodec.avroEncoder
  val avroValDecoder: AvroDecoder[V] = serdeOfVal.avroCodec.avroDecoder

  val schemaForKey: SchemaFor[K] = serdeOfKey.avroCodec.schemaFor
  val schemaForVal: SchemaFor[V] = serdeOfVal.avroCodec.schemaFor

  def in[F[_]](ctx: KafkaContext[F]): KafkaTopic[F, K, V] =
    ctx.topic[K, V](this)
}

object TopicDef {

  implicit def showTopicDef[K, V]: Show[TopicDef[K, V]] = _.toString

  implicit def eqTopicDef[K, V]: Eq[TopicDef[K, V]] =
    (x: TopicDef[K, V], y: TopicDef[K, V]) =>
      x.topicName.value === y.topicName.value &&
        x.schemaForKey.schema == y.schemaForKey.schema &&
        x.schemaForVal.schema == y.schemaForVal.schema

  def apply[K, V](
    topicName: TopicName,
    keySchema: AvroCodec[K],
    valueSchema: AvroCodec[V]): TopicDef[K, V] =
    new TopicDef(topicName)(SerdeOf(keySchema), SerdeOf(valueSchema))

  def apply[K: SerdeOf, V: SerdeOf](topicName: TopicName): TopicDef[K, V] =
    new TopicDef(topicName)(SerdeOf[K], SerdeOf[V])

  def apply[K: SerdeOf, V](topicName: TopicName, valueSchema: AvroCodec[V]): TopicDef[K, V] =
    new TopicDef(topicName)(SerdeOf[K], SerdeOf(valueSchema))
}
