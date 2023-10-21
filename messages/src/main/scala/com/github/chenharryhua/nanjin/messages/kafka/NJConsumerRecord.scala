package com.github.chenharryhua.nanjin.messages.kafka

import cats.kernel.PartialOrder
import cats.syntax.all.*
import cats.{Bifunctor, Eq, Show}
import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import com.sksamuel.avro4s.*
import fs2.kafka.{ConsumerRecord, Header as Fs2Header}
import io.circe.generic.JsonCodec
import io.scalaland.chimney.dsl.*
import org.apache.avro.Schema
import org.apache.kafka.clients.consumer.ConsumerRecord as JavaConsumerRecord

import java.time.{Instant, ZoneId, ZonedDateTime}
import scala.annotation.nowarn

@JsonCodec
@AvroName("header")
@AvroNamespace("nanjin.kafka")
final case class NJHeader(key: String, value: Array[Byte])
object NJHeader {
  // consistent with fs2.kafka
  implicit val showNJHeader: Show[NJHeader] = (a: NJHeader) => Fs2Header(a.key, a.value).show
  implicit val eqNJHeader: Eq[NJHeader] = (x: NJHeader, y: NJHeader) =>
    Fs2Header(x.key, x.value) === Fs2Header(y.key, y.value)
}

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

  implicit val bifunctorOptionalKV: Bifunctor[NJConsumerRecord] =
    new Bifunctor[NJConsumerRecord] {

      override def bimap[A, B, C, D](
        fab: NJConsumerRecord[A, B])(f: A => C, g: B => D): NJConsumerRecord[C, D] =
        fab.copy(key = fab.key.map(f), value = fab.value.map(g))
    }

  implicit def partialOrderOptionalKV[K, V]: PartialOrder[NJConsumerRecord[K, V]] =
    (x: NJConsumerRecord[K, V], y: NJConsumerRecord[K, V]) =>
      if (x.partition === y.partition) {
        if (x.offset < y.offset) -1.0 else if (x.offset > y.offset) 1.0 else 0.0
      } else Double.NaN
}
