package com.github.chenharryhua.nanjin.kafka

import cats.Show
import cats.kernel.Eq
import cats.syntax.eq.*
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.streaming.{NJStateStore, StreamingChannel}
import com.github.chenharryhua.nanjin.messages.kafka.codec.{AvroCodec, SerdeOf}
import com.sksamuel.avro4s.{SchemaFor, Decoder as AvroDecoder, Encoder as AvroEncoder}
import org.apache.kafka.streams.state.StateSerdes

final class TopicDef[K, V] private (val topicName: TopicName, val serdeOfKey: SerdeOf[K], val serdeOfVal: SerdeOf[V])
    extends Serializable {

  override def toString: String = topicName.value

  def withTopicName(tn: String): TopicDef[K, V] =
    new TopicDef[K, V](TopicName.unsafeFrom(tn), serdeOfKey, serdeOfVal)

  val avroKeyEncoder: AvroEncoder[K] = serdeOfKey.avroCodec.avroEncoder
  val avroKeyDecoder: AvroDecoder[K] = serdeOfKey.avroCodec.avroDecoder

  val avroValEncoder: AvroEncoder[V] = serdeOfVal.avroCodec.avroEncoder
  val avroValDecoder: AvroDecoder[V] = serdeOfVal.avroCodec.avroDecoder

  val schemaForKey: SchemaFor[K] = serdeOfKey.avroCodec.schemaFor
  val schemaForVal: SchemaFor[V] = serdeOfVal.avroCodec.schemaFor

  def in[F[_]](ctx: KafkaContext[F]): KafkaTopic[F, K, V] = ctx.topic[K, V](this)

  def stateSerdes: StateSerdes[K, V] = new StateSerdes[K, V](topicName.value, serdeOfKey, serdeOfVal)

  def asStateStore(name: String): NJStateStore[K, V] = {
    require(name =!= topicName.value, "should provide a name other than the topic name")
    NJStateStore[K, V](name)(serdeOfKey, serdeOfVal)
  }
  def kafkaStream: StreamingChannel[K, V] = new StreamingChannel[K, V](this)
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
    new TopicDef(topicName, sk, sv)
  }

  def apply[K: SerdeOf, V: SerdeOf](topicName: TopicName): TopicDef[K, V] = {
    val sk = SerdeOf[K]
    val sv = SerdeOf[V]
    new TopicDef(topicName, sk, sv)
  }

  def apply[K: SerdeOf, V](topicName: TopicName, valSchema: AvroCodec[V]): TopicDef[K, V] = {
    val sk = SerdeOf[K]
    val sv = SerdeOf(valSchema)
    new TopicDef(topicName, sk, sv)
  }
}
