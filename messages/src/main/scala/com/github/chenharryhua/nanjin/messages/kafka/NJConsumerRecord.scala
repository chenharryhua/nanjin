package com.github.chenharryhua.nanjin.messages.kafka

import cats.Bifunctor
import cats.kernel.Eq
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import com.sksamuel.avro4s.*
import fs2.kafka.ConsumerRecord
import io.circe.{Decoder as JsonDecoder, Encoder as JsonEncoder}
import io.scalaland.chimney.dsl.*
import org.apache.avro.Schema
import org.apache.kafka.clients.consumer.ConsumerRecord as JavaConsumerRecord

import java.time.{Instant, ZoneId, ZonedDateTime}
import scala.annotation.nowarn

@AvroDoc("kafka consumer record, optional Key and optional Value")
@AvroNamespace("nanjin.kafka")
@AvroName("NJConsumerRecord")
final case class NJConsumerRecord[K, V](
  @AvroDoc("kafka topic name") topic: String,
  @AvroDoc("kafka partition") partition: Int,
  @AvroDoc("kafka offset") offset: Long,
  @AvroDoc("kafka timestamp in millisecond") timestamp: Long,
  @AvroDoc("kafka timestamp type") timestampType: Int,
  @AvroDoc("kafka key size") serializedKeySize: Option[Int],
  @AvroDoc("kafka value size") serializedValueSize: Option[Int],
  @AvroDoc("kafka key") key: Option[K],
  @AvroDoc("kafka value") value: Option[V],
  @AvroDoc("kafka headers") headers: List[NJHeader],
  @AvroDoc("kafka leader epoch") leaderEpoch: Option[Int]) {

  def flatten[K2, V2](implicit
    evK: K <:< Option[K2],
    evV: V <:< Option[V2]
  ): NJConsumerRecord[K2, V2] =
    copy(key = key.flatten, value = value.flatten)

  def toNJProducerRecord: NJProducerRecord[K, V] =
    NJProducerRecord[K, V](topic, Some(partition), Some(offset), Some(timestamp), key, value, headers)

  def toJavaConsumerRecord: JavaConsumerRecord[K, V] = this.transformInto[JavaConsumerRecord[K, V]]
  def toConsumerRecord: ConsumerRecord[K, V]         = this.transformInto[ConsumerRecord[K, V]]

  def metaInfo(zoneId: ZoneId): RecordMetaInfo =
    this
      .into[RecordMetaInfo]
      .withFieldComputed(
        _.timestamp,
        ts => ZonedDateTime.ofInstant(Instant.ofEpochMilli(ts.timestamp), zoneId))
      .transform
}

object NJConsumerRecord extends NJConsumerRecordTransformers {

  def apply[K, V](cr: JavaConsumerRecord[K, V]): NJConsumerRecord[K, V] =
    cr.transformInto[NJConsumerRecord[K, V]]

  def apply[K, V](cr: ConsumerRecord[K, V]): NJConsumerRecord[K, V] =
    cr.transformInto[NJConsumerRecord[K, V]]

  def avroCodec[K, V](
    keyCodec: NJAvroCodec[K],
    valCodec: NJAvroCodec[V]): NJAvroCodec[NJConsumerRecord[K, V]] = {
    @nowarn implicit val schemaForKey: SchemaFor[K] = keyCodec.schemaFor
    @nowarn implicit val schemaForVal: SchemaFor[V] = valCodec.schemaFor
    @nowarn implicit val keyDecoder: Decoder[K]     = keyCodec
    @nowarn implicit val valDecoder: Decoder[V]     = valCodec
    @nowarn implicit val keyEncoder: Encoder[K]     = keyCodec
    @nowarn implicit val valEncoder: Encoder[V]     = valCodec
    val s: SchemaFor[NJConsumerRecord[K, V]]        = implicitly
    val d: Decoder[NJConsumerRecord[K, V]]          = implicitly
    val e: Encoder[NJConsumerRecord[K, V]]          = implicitly
    NJAvroCodec[NJConsumerRecord[K, V]](s, d.withSchema(s), e.withSchema(s))
  }

  def schema(keySchema: Schema, valSchema: Schema): Schema = {
    class KEY
    class VAL
    @nowarn
    implicit val schemaForKey: SchemaFor[KEY] = new SchemaFor[KEY] {
      override def schema: Schema           = keySchema
      override def fieldMapper: FieldMapper = DefaultFieldMapper
    }

    @nowarn
    implicit val schemaForVal: SchemaFor[VAL] = new SchemaFor[VAL] {
      override def schema: Schema           = valSchema
      override def fieldMapper: FieldMapper = DefaultFieldMapper
    }
    SchemaFor[NJConsumerRecord[KEY, VAL]].schema
  }

  @nowarn
  implicit def encoderNJConsumerRecord[K: JsonEncoder, V: JsonEncoder]: JsonEncoder[NJConsumerRecord[K, V]] =
    io.circe.generic.semiauto.deriveEncoder[NJConsumerRecord[K, V]]

  @nowarn
  implicit def decoderNJConsumerRecord[K: JsonDecoder, V: JsonDecoder]: JsonDecoder[NJConsumerRecord[K, V]] =
    io.circe.generic.semiauto.deriveDecoder[NJConsumerRecord[K, V]]

  implicit val bifunctorNJConsumerRecord: Bifunctor[NJConsumerRecord] =
    new Bifunctor[NJConsumerRecord] {

      override def bimap[A, B, C, D](
        fab: NJConsumerRecord[A, B])(f: A => C, g: B => D): NJConsumerRecord[C, D] =
        fab.copy(key = fab.key.map(f), value = fab.value.map(g))
    }

  implicit def eqNJConsumerRecord[K: Eq, V: Eq]: Eq[NJConsumerRecord[K, V]] =
    Eq.instance { case (l, r) =>
      l.topic === r.topic &&
      l.partition === r.partition &&
      l.offset === r.offset &&
      l.timestamp === r.timestamp &&
      l.timestampType === r.timestampType &&
      l.serializedKeySize === r.serializedKeySize &&
      l.serializedValueSize === r.serializedValueSize &&
      l.key === r.key &&
      l.value === r.value &&
      l.headers === r.headers &&
      l.leaderEpoch === r.leaderEpoch
    }
}
