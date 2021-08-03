package com.github.chenharryhua.nanjin.kafka

import cats.Show
import cats.kernel.Eq
import cats.syntax.eq.*
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.streaming.NJStateStore
import com.github.chenharryhua.nanjin.messages.kafka.codec.{AvroCodec, SerdeOf}
import com.sksamuel.avro4s.{SchemaFor, Decoder as AvroDecoder, Encoder as AvroEncoder}
import org.apache.kafka.streams.Topology.AutoOffsetReset
import org.apache.kafka.streams.kstream.Consumed as JConsumed
import org.apache.kafka.streams.processor.TimestampExtractor
import org.apache.kafka.streams.scala.kstream.Consumed
import org.apache.kafka.streams.state.StateSerdes

final class TopicDef[K, V] private (
  val topicName: TopicName,
  val consumed: JConsumed[K, V],
  val serdeOfKey: SerdeOf[K],
  val serdeOfVal: SerdeOf[V])
    extends Serializable {

  override def toString: String = topicName.value

  def withTopicName(tn: String): TopicDef[K, V] =
    new TopicDef[K, V](TopicName.unsafeFrom(tn), consumed, serdeOfKey, serdeOfVal)

  val avroKeyEncoder: AvroEncoder[K] = serdeOfKey.avroCodec.avroEncoder
  val avroKeyDecoder: AvroDecoder[K] = serdeOfKey.avroCodec.avroDecoder

  val avroValEncoder: AvroEncoder[V] = serdeOfVal.avroCodec.avroEncoder
  val avroValDecoder: AvroDecoder[V] = serdeOfVal.avroCodec.avroDecoder

  val schemaForKey: SchemaFor[K] = serdeOfKey.avroCodec.schemaFor
  val schemaForVal: SchemaFor[V] = serdeOfVal.avroCodec.schemaFor

  def in[F[_]](ctx: KafkaContext[F]): KafkaTopic[F, K, V] = ctx.topic[K, V](this)

  private def updateConsumed(c: JConsumed[K, V]): TopicDef[K, V] =
    new TopicDef[K, V](topicName, c, serdeOfKey, serdeOfVal)

  def withConsumed(timestampExtractor: TimestampExtractor, resetPolicy: AutoOffsetReset): TopicDef[K, V] =
    updateConsumed(Consumed.`with`(timestampExtractor, resetPolicy)(serdeOfKey, serdeOfVal))

  def withConsumed(timestampExtractor: TimestampExtractor): TopicDef[K, V] =
    updateConsumed(Consumed.`with`(timestampExtractor)(serdeOfKey, serdeOfVal))

  def withConsumed(resetPolicy: AutoOffsetReset): TopicDef[K, V] =
    updateConsumed(Consumed.`with`(resetPolicy)(serdeOfKey, serdeOfVal))

  def stateSerdes: StateSerdes[K, V] = new StateSerdes[K, V](topicName.value, serdeOfKey, serdeOfVal)

  def asStateStore(name: String): NJStateStore[K, V] = {
    require(name =!= topicName.value, "should provide a name other than the topic name")
    NJStateStore[K, V](name)(serdeOfKey, serdeOfVal)
  }
}

object TopicDef {

  implicit def showTopicDef[K, V]: Show[TopicDef[K, V]] = _.toString

  implicit def eqTopicDef[K, V]: Eq[TopicDef[K, V]] =
    (x: TopicDef[K, V], y: TopicDef[K, V]) =>
      x.topicName.value === y.topicName.value &&
        x.schemaForKey.schema == y.schemaForKey.schema &&
        x.schemaForVal.schema == y.schemaForVal.schema

  def apply[K, V](topicName: TopicName, keySchema: AvroCodec[K], valueSchema: AvroCodec[V]): TopicDef[K, V] = {
    val sk = SerdeOf(keySchema)
    val sv = SerdeOf(valueSchema)
    new TopicDef(topicName, Consumed.`with`(sk, sv), sk, sv)
  }

  def apply[K: SerdeOf, V: SerdeOf](topicName: TopicName): TopicDef[K, V] = {
    val sk = SerdeOf[K]
    val sv = SerdeOf[V]
    new TopicDef(topicName, Consumed.`with`(sk, sv), sk, sv)
  }

  def apply[K: SerdeOf, V](topicName: TopicName, valueSchema: AvroCodec[V]): TopicDef[K, V] = {
    val sk = SerdeOf[K]
    val sv = SerdeOf(valueSchema)
    new TopicDef(topicName, Consumed.`with`(sk, sv), sk, sv)
  }
}
