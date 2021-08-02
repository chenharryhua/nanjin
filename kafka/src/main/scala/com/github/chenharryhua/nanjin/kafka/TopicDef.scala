package com.github.chenharryhua.nanjin.kafka

import cats.Show
import cats.syntax.eq.*
import cats.kernel.Eq
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.messages.kafka.codec.{AvroCodec, SerdeOf}
import com.sksamuel.avro4s.{SchemaFor, Decoder as AvroDecoder, Encoder as AvroEncoder}
import org.apache.kafka.streams.Topology.AutoOffsetReset
import org.apache.kafka.streams.kstream.{Consumed as JConsumed, Materialized as JMaterialized}
import org.apache.kafka.streams.processor.{StateStore, TimestampExtractor}
import org.apache.kafka.streams.scala.kstream.{Consumed, Materialized}
import org.apache.kafka.streams.scala.{ByteArrayKeyValueStore, ByteArraySessionStore, ByteArrayWindowStore}
import org.apache.kafka.streams.state.{KeyValueBytesStoreSupplier, SessionBytesStoreSupplier, WindowBytesStoreSupplier}

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

  def in[F[_]](ctx: KafkaContext[F]): KafkaTopic[F, K, V] = ctx.topic[K, V](this)

  def materialized[S <: StateStore](storeName: String): JMaterialized[K, V, S] =
    Materialized.as[K, V, S](storeName)(serdeOfKey, serdeOfVal)
  def materialized(supplier: WindowBytesStoreSupplier): JMaterialized[K, V, ByteArrayWindowStore] =
    Materialized.as[K, V](supplier)(serdeOfKey, serdeOfVal)
  def materialized(supplier: SessionBytesStoreSupplier): JMaterialized[K, V, ByteArraySessionStore] =
    Materialized.as[K, V](supplier)(serdeOfKey, serdeOfVal)
  def materialized(supplier: KeyValueBytesStoreSupplier): JMaterialized[K, V, ByteArrayKeyValueStore] =
    Materialized.as[K, V](supplier)(serdeOfKey, serdeOfVal)

  def consumed(timestampExtractor: TimestampExtractor, resetPolicy: AutoOffsetReset): JConsumed[K, V] =
    Consumed.`with`(timestampExtractor, resetPolicy)(serdeOfKey, serdeOfVal)

  def consumed(timestampExtractor: TimestampExtractor): JConsumed[K, V] =
    Consumed.`with`(timestampExtractor)(serdeOfKey, serdeOfVal)

  def consumed(resetPolicy: AutoOffsetReset): JConsumed[K, V] =
    Consumed.`with`(resetPolicy)(serdeOfKey, serdeOfVal)

  def consumed: JConsumed[K, V] = Consumed.`with`(serdeOfKey, serdeOfVal)

}

object TopicDef {

  implicit def showTopicDef[K, V]: Show[TopicDef[K, V]] = _.toString

  implicit def eqTopicDef[K, V]: Eq[TopicDef[K, V]] =
    (x: TopicDef[K, V], y: TopicDef[K, V]) =>
      x.topicName.value === y.topicName.value &&
        x.schemaForKey.schema == y.schemaForKey.schema &&
        x.schemaForVal.schema == y.schemaForVal.schema

  def apply[K, V](topicName: TopicName, keySchema: AvroCodec[K], valueSchema: AvroCodec[V]): TopicDef[K, V] =
    new TopicDef(topicName)(SerdeOf(keySchema), SerdeOf(valueSchema))

  def apply[K: SerdeOf, V: SerdeOf](topicName: TopicName): TopicDef[K, V] =
    new TopicDef(topicName)(SerdeOf[K], SerdeOf[V])

  def apply[K: SerdeOf, V](topicName: TopicName, valueSchema: AvroCodec[V]): TopicDef[K, V] =
    new TopicDef(topicName)(SerdeOf[K], SerdeOf(valueSchema))

}
