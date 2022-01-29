package com.github.chenharryhua.nanjin.spark.kafka

import cats.Bifunctor
import cats.implicits.catsSyntaxTuple2Semigroupal
import cats.kernel.PartialOrder
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.datetime.NJTimestamp
import com.github.chenharryhua.nanjin.kafka.TopicDef
import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import com.github.chenharryhua.nanjin.messages.kafka.instances.toJavaConsumerRecordTransformer
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import com.sksamuel.avro4s.*
import frameless.TypedEncoder
import fs2.kafka.ConsumerRecord as Fs2ConsumerRecord
import io.circe.generic.auto.*
import io.circe.{Decoder as JsonDecoder, Encoder as JsonEncoder, Json}
import io.scalaland.chimney.dsl.*
import monocle.Optional
import monocle.macros.Lenses
import monocle.std.option.some
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.record.TimestampType
import shapeless.cachedImplicit

@Lenses
@AvroDoc("kafka record, optional Key and Value")
@AvroNamespace("nj.spark.kafka")
@AvroName("NJConsumerRecord")
final case class NJConsumerRecord[K, V](
  @AvroDoc("kafka partition") partition: Int,
  @AvroDoc("kafka offset") offset: Long,
  @AvroDoc("kafka timestamp in millisecond") timestamp: Long,
  @AvroDoc("kafka key") key: Option[K],
  @AvroDoc("kafka value") value: Option[V],
  @AvroDoc("kafka topic") topic: String,
  @AvroDoc("kafka timestamp type") timestampType: Int) {

  def newKey[K2](key: Option[K2]): NJConsumerRecord[K2, V]     = copy(key = key)
  def newValue[V2](value: Option[V2]): NJConsumerRecord[K, V2] = copy(value = value)

  def flatten[K2, V2](implicit
    evK: K <:< Option[K2],
    evV: V <:< Option[V2]
  ): NJConsumerRecord[K2, V2] =
    copy(key = key.flatten, value = value.flatten)

  def toNJProducerRecord: NJProducerRecord[K, V] =
    NJProducerRecord[K, V](Some(partition), Some(offset), Some(timestamp), key, value)

  def asJson(implicit k: JsonEncoder[K], v: JsonEncoder[V]): Json =
    JsonEncoder[NJConsumerRecord[K, V]].apply(this)

  private def tst: TimestampType = timestampType match {
    case 0 => TimestampType.CREATE_TIME
    case 1 => TimestampType.LOG_APPEND_TIME
    case _ => TimestampType.NO_TIMESTAMP_TYPE
  }

  def metaInfo: String =
    s"Meta(topic=$topic,partition=$partition,offset=$offset,ts=${NJTimestamp(timestamp).utc},tt=${tst.toString})"

  override def toString: String =
    s"CR($metaInfo,key=${key.toString},value=${value.toString})"
}

object NJConsumerRecord {

  def optionalKey[K, V]: Optional[NJConsumerRecord[K, V], K]   = NJConsumerRecord.key[K, V].composePrism(some)
  def optionalValue[K, V]: Optional[NJConsumerRecord[K, V], V] = NJConsumerRecord.value[K, V].composePrism(some)

  def apply[K, V](cr: ConsumerRecord[Option[K], Option[V]]): NJConsumerRecord[K, V] =
    NJConsumerRecord(cr.partition, cr.offset, cr.timestamp, cr.key, cr.value, cr.topic, cr.timestampType.id)

  def apply[K, V](cr: Fs2ConsumerRecord[Option[K], Option[V]]): NJConsumerRecord[K, V] =
    apply(cr.transformInto[ConsumerRecord[Option[K], Option[V]]])

  def avroCodec[K, V](keyCodec: NJAvroCodec[K], valCodec: NJAvroCodec[V]): NJAvroCodec[NJConsumerRecord[K, V]] = {
    implicit val schemaForKey: SchemaFor[K]  = keyCodec.schemaFor
    implicit val schemaForVal: SchemaFor[V]  = valCodec.schemaFor
    implicit val keyDecoder: Decoder[K]      = keyCodec.avroDecoder
    implicit val valDecoder: Decoder[V]      = valCodec.avroDecoder
    implicit val keyEncoder: Encoder[K]      = keyCodec.avroEncoder
    implicit val valEncoder: Encoder[V]      = valCodec.avroEncoder
    val s: SchemaFor[NJConsumerRecord[K, V]] = cachedImplicit
    val d: Decoder[NJConsumerRecord[K, V]]   = cachedImplicit
    val e: Encoder[NJConsumerRecord[K, V]]   = cachedImplicit
    NJAvroCodec[NJConsumerRecord[K, V]](s, d.withSchema(s), e.withSchema(s))
  }

  def avroCodec[K, V](topicDef: TopicDef[K, V]): NJAvroCodec[NJConsumerRecord[K, V]] =
    avroCodec(topicDef.rawSerdes.keySerde.avroCodec, topicDef.rawSerdes.valSerde.avroCodec)

  def ate[K, V](keyCodec: NJAvroCodec[K], valCodec: NJAvroCodec[V])(implicit
    tek: TypedEncoder[K],
    tev: TypedEncoder[V]): AvroTypedEncoder[NJConsumerRecord[K, V]] = {
    val ote: TypedEncoder[NJConsumerRecord[K, V]] = shapeless.cachedImplicit
    AvroTypedEncoder[NJConsumerRecord[K, V]](ote, avroCodec(keyCodec, valCodec))
  }

  def ate[K, V](topicDef: TopicDef[K, V])(implicit
    tek: TypedEncoder[K],
    tev: TypedEncoder[V]): AvroTypedEncoder[NJConsumerRecord[K, V]] =
    ate(topicDef.rawSerdes.keySerde.avroCodec, topicDef.rawSerdes.valSerde.avroCodec)

  implicit def jsonEncoder[K, V](implicit
    jck: JsonEncoder[K],
    jcv: JsonEncoder[V]): JsonEncoder[NJConsumerRecord[K, V]] =
    io.circe.generic.semiauto.deriveEncoder[NJConsumerRecord[K, V]]

  implicit def jsonDecoder[K, V](implicit
    jck: JsonDecoder[K],
    jcv: JsonDecoder[V]): JsonDecoder[NJConsumerRecord[K, V]] =
    io.circe.generic.semiauto.deriveDecoder[NJConsumerRecord[K, V]]

  implicit val bifunctorOptionalKV: Bifunctor[NJConsumerRecord] =
    new Bifunctor[NJConsumerRecord] {

      override def bimap[A, B, C, D](fab: NJConsumerRecord[A, B])(f: A => C, g: B => D): NJConsumerRecord[C, D] =
        fab.copy(key = fab.key.map(f), value = fab.value.map(g))
    }

  implicit def partialOrderOptionlKV[K, V]: PartialOrder[NJConsumerRecord[K, V]] =
    (x: NJConsumerRecord[K, V], y: NJConsumerRecord[K, V]) =>
      if (x.partition === y.partition) {
        if (x.offset < y.offset) -1.0 else if (x.offset > y.offset) 1.0 else 0.0
      } else Double.NaN
}
