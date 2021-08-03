package com.github.chenharryhua.nanjin.kafka

import cats.Show
import cats.kernel.Eq
import cats.syntax.eq.*
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.messages.kafka.codec.{AvroCodec, SerdeOf}
import com.sksamuel.avro4s.{SchemaFor, Decoder as AvroDecoder, Encoder as AvroEncoder}

final class TopicDef[K, V] private (val topicName: TopicName, val serdePair: RawKeyValueSerdePair[K, V])
    extends Serializable {

  override def toString: String = topicName.value

  def withTopicName(tn: String): TopicDef[K, V] =
    new TopicDef[K, V](TopicName.unsafeFrom(tn), serdePair)

  val avroKeyEncoder: AvroEncoder[K] = serdePair.key.avroCodec.avroEncoder
  val avroKeyDecoder: AvroDecoder[K] = serdePair.key.avroCodec.avroDecoder

  val avroValEncoder: AvroEncoder[V] = serdePair.value.avroCodec.avroEncoder
  val avroValDecoder: AvroDecoder[V] = serdePair.value.avroCodec.avroDecoder

  val schemaForKey: SchemaFor[K] = serdePair.key.avroCodec.schemaFor
  val schemaForVal: SchemaFor[V] = serdePair.value.avroCodec.schemaFor

  def in[F[_]](ctx: KafkaContext[F]): KafkaTopic[F, K, V] = ctx.topic[K, V](this)

}

object TopicDef {

  implicit def showTopicDef[K, V]: Show[TopicDef[K, V]] = _.toString

  implicit def eqTopicDef[K, V]: Eq[TopicDef[K, V]] =
    (x: TopicDef[K, V], y: TopicDef[K, V]) =>
      x.topicName.value === y.topicName.value &&
        x.schemaForKey.schema == y.schemaForKey.schema &&
        x.schemaForVal.schema == y.schemaForVal.schema

  def apply[K, V](topicName: TopicName, keySchema: AvroCodec[K], valSchema: AvroCodec[V]): TopicDef[K, V] = {
    val sk = SerdeOf(keySchema)
    val sv = SerdeOf(valSchema)
    new TopicDef(topicName, RawKeyValueSerdePair(sk, sv))
  }

  def apply[K: SerdeOf, V: SerdeOf](topicName: TopicName): TopicDef[K, V] = {
    val sk = SerdeOf[K]
    val sv = SerdeOf[V]
    new TopicDef(topicName, RawKeyValueSerdePair(sk, sv))
  }

  def apply[K: SerdeOf, V](topicName: TopicName, valSchema: AvroCodec[V]): TopicDef[K, V] = {
    val sk = SerdeOf[K]
    val sv = SerdeOf(valSchema)
    new TopicDef(topicName, RawKeyValueSerdePair(sk, sv))
  }
}
