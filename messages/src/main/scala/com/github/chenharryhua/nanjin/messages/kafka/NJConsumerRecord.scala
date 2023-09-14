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
import org.apache.kafka.clients.consumer.ConsumerRecord as KafkaConsumerRecord
import org.apache.kafka.common.header.Headers as KafkaHeaders

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

  def apply(headers: KafkaHeaders): List[NJHeader] =
    headers.toArray.map(h => NJHeader(h.key(), h.value())).toList
}

@AvroDoc("kafka consumer record, optional Key and optional Value")
@AvroNamespace("nanjin.kafka")
@AvroName("NJConsumerRecord")
final case class NJConsumerRecord[K, V](
  @AvroDoc("kafka partition") partition: Int,
  @AvroDoc("kafka offset") offset: Long,
  @AvroDoc("kafka timestamp in millisecond") timestamp: Long,
  @AvroDoc("kafka key") key: Option[K],
  @AvroDoc("kafka value") value: Option[V],
  @AvroDoc("kafka topic name") topic: String,
  @AvroDoc("kafka timestamp type") timestampType: Int,
  @AvroDoc("kafka headers") headers: List[NJHeader]) {

  def flatten[K2, V2](implicit
    evK: K <:< Option[K2],
    evV: V <:< Option[V2]
  ): NJConsumerRecord[K2, V2] =
    copy(key = key.flatten, value = value.flatten)

  def toNJProducerRecord: NJProducerRecord[K, V] =
    NJProducerRecord[K, V](topic, Some(partition), Some(offset), Some(timestamp), key, value, headers)

  def metaInfo(zoneId: ZoneId): RecordMetaInfo =
    this
      .into[RecordMetaInfo]
      .withFieldComputed(_.timestamp, x => ZonedDateTime.ofInstant(Instant.ofEpochMilli(x.timestamp), zoneId))
      .transform

}

object NJConsumerRecord extends Isos {

  def apply[K, V](cr: KafkaConsumerRecord[Option[K], Option[V]]): NJConsumerRecord[K, V] =
    NJConsumerRecord(
      partition = cr.partition,
      offset = cr.offset,
      timestamp = cr.timestamp,
      key = cr.key,
      value = cr.value,
      topic = cr.topic,
      timestampType = cr.timestampType.id,
      headers = NJHeader(cr.headers())
    )

  def apply[K, V](cr: ConsumerRecord[Option[K], Option[V]]): NJConsumerRecord[K, V] =
    apply(isoFs2ComsumerRecord.get(cr))

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
