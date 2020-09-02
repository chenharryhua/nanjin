package com.github.chenharryhua.nanjin.kafka

import cats.implicits._
import cats.kernel.Eq
import com.github.chenharryhua.nanjin.messages.kafka._
import com.github.chenharryhua.nanjin.messages.kafka.codec.{NJAvroCodec, SerdeOf}
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

  val keySchemaFor: SchemaFor[K] = serdeOfKey.avroCodec.schemaFor
  val valSchemaFor: SchemaFor[V] = serdeOfVal.avroCodec.schemaFor

  implicit private val ks: SchemaFor[K]   = keySchemaFor
  implicit private val vs: SchemaFor[V]   = valSchemaFor
  implicit private val kd: AvroDecoder[K] = avroKeyDecoder
  implicit private val vd: AvroDecoder[V] = avroValDecoder
  implicit private val ke: AvroEncoder[K] = avroKeyEncoder
  implicit private val ve: AvroEncoder[V] = avroValEncoder

  val schemaForOptionalKV: SchemaFor[OptionalKV[K, V]] =
    shapeless.cachedImplicit[SchemaFor[OptionalKV[K, V]]]

  val schemaForCompulsoryK: SchemaFor[CompulsoryK[K, V]] =
    shapeless.cachedImplicit[SchemaFor[CompulsoryK[K, V]]]

  val schemaForCompulsoryV: SchemaFor[CompulsoryV[K, V]] =
    shapeless.cachedImplicit[SchemaFor[CompulsoryV[K, V]]]

  val schemaForCompulsoryKV: SchemaFor[CompulsoryKV[K, V]] =
    shapeless.cachedImplicit[SchemaFor[CompulsoryKV[K, V]]]

  val optionalKVDecoder: AvroDecoder[OptionalKV[K, V]] =
    shapeless.cachedImplicit[AvroDecoder[OptionalKV[K, V]]]

  val compulsoryKDecoder: AvroDecoder[CompulsoryK[K, V]] =
    shapeless.cachedImplicit[AvroDecoder[CompulsoryK[K, V]]]

  val compulsoryVDecoder: AvroDecoder[CompulsoryV[K, V]] =
    shapeless.cachedImplicit[AvroDecoder[CompulsoryV[K, V]]]

  val compulsoryKVDecoder: AvroDecoder[CompulsoryKV[K, V]] =
    shapeless.cachedImplicit[AvroDecoder[CompulsoryKV[K, V]]]

  val optionalKVEncoder: AvroEncoder[OptionalKV[K, V]] =
    shapeless.cachedImplicit[AvroEncoder[OptionalKV[K, V]]]

  val compulsoryKEncoder: AvroEncoder[CompulsoryK[K, V]] =
    shapeless.cachedImplicit[AvroEncoder[CompulsoryK[K, V]]]

  val compulsoryVEncoder: AvroEncoder[CompulsoryV[K, V]] =
    shapeless.cachedImplicit[AvroEncoder[CompulsoryV[K, V]]]

  val compulsoryKVEncoder: AvroEncoder[CompulsoryKV[K, V]] =
    shapeless.cachedImplicit[AvroEncoder[CompulsoryKV[K, V]]]

  val optionalKVCodec: NJAvroCodec[OptionalKV[K, V]] =
    NJAvroCodec[OptionalKV[K, V]](schemaForOptionalKV, optionalKVDecoder, optionalKVEncoder)

  val compulsoryKCodec: NJAvroCodec[CompulsoryK[K, V]] =
    NJAvroCodec[CompulsoryK[K, V]](schemaForCompulsoryK, compulsoryKDecoder, compulsoryKEncoder)

  val compulsoryVCodec: NJAvroCodec[CompulsoryV[K, V]] =
    NJAvroCodec[CompulsoryV[K, V]](schemaForCompulsoryV, compulsoryVDecoder, compulsoryVEncoder)

  val compulsoryKVCodec: NJAvroCodec[CompulsoryKV[K, V]] =
    NJAvroCodec[CompulsoryKV[K, V]](schemaForCompulsoryKV, compulsoryKVDecoder, compulsoryKVEncoder)

  def in[F[_]](ctx: KafkaContext[F]): KafkaTopic[F, K, V] =
    ctx.topic[K, V](this)
}

object TopicDef {

  implicit def eqTopicDef[K, V]: Eq[TopicDef[K, V]] =
    (x: TopicDef[K, V], y: TopicDef[K, V]) =>
      x.topicName.value === y.topicName.value && x.schemaForOptionalKV == y.schemaForOptionalKV

  def apply[K, V](
    topicName: TopicName,
    keySchema: NJAvroCodec[K],
    valueSchema: NJAvroCodec[V]): TopicDef[K, V] =
    new TopicDef(topicName)(SerdeOf(keySchema), SerdeOf(valueSchema))

  def apply[K: SerdeOf, V: SerdeOf](topicName: TopicName): TopicDef[K, V] =
    new TopicDef(topicName)(SerdeOf[K], SerdeOf[V])

  def apply[K: SerdeOf, V](topicName: TopicName, valueSchema: NJAvroCodec[V]): TopicDef[K, V] =
    new TopicDef(topicName)(SerdeOf[K], SerdeOf(valueSchema))
}
