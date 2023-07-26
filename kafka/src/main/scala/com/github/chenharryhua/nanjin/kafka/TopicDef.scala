package com.github.chenharryhua.nanjin.kafka

import cats.Show
import cats.kernel.Eq
import cats.syntax.eq.*
import com.github.chenharryhua.nanjin.common.kafka.{TopicName, TopicNameC}
import com.github.chenharryhua.nanjin.messages.kafka.codec.{NJAvroCodec, SerdeOf}
import com.sksamuel.avro4s.{Decoder as AvroDecoder, Encoder as AvroEncoder, SchemaFor}

final class TopicDef[K, V] private (val topicName: TopicName, val rawSerdes: RawKeyValueSerdePair[K, V])
    extends Serializable {

  override def toString: String = topicName.value

  def withTopicName(tn: TopicName): TopicDef[K, V]  = new TopicDef[K, V](tn, rawSerdes)
  def withTopicName(tn: TopicNameC): TopicDef[K, V] = withTopicName(TopicName(tn))

  val avroKeyEncoder: AvroEncoder[K] = rawSerdes.keySerde.avroCodec.avroEncoder
  val avroKeyDecoder: AvroDecoder[K] = rawSerdes.keySerde.avroCodec.avroDecoder

  val avroValEncoder: AvroEncoder[V] = rawSerdes.valSerde.avroCodec.avroEncoder
  val avroValDecoder: AvroDecoder[V] = rawSerdes.valSerde.avroCodec.avroDecoder

  val schemaForKey: SchemaFor[K] = rawSerdes.keySerde.avroCodec.schemaFor
  val schemaForVal: SchemaFor[V] = rawSerdes.valSerde.avroCodec.schemaFor

  def in[F[_]](ctx: KafkaContext[F]): KafkaTopic[F, K, V] = ctx.topic[K, V](this)

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
    keySchema: NJAvroCodec[K],
    valSchema: NJAvroCodec[V]): TopicDef[K, V] = {
    val sk = SerdeOf(keySchema)
    val sv = SerdeOf(valSchema)
    new TopicDef(topicName, RawKeyValueSerdePair(sk, sv))
  }

  def apply[K: SerdeOf, V: SerdeOf](topicName: TopicName): TopicDef[K, V] = {
    val sk = SerdeOf[K]
    val sv = SerdeOf[V]
    new TopicDef(topicName, RawKeyValueSerdePair(sk, sv))
  }

  def apply[K: SerdeOf, V](topicName: TopicName, valSchema: NJAvroCodec[V]): TopicDef[K, V] = {
    val sk = SerdeOf[K]
    val sv = SerdeOf(valSchema)
    new TopicDef(topicName, RawKeyValueSerdePair(sk, sv))
  }
}
