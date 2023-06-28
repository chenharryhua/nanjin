package com.github.chenharryhua.nanjin.messages.kafka

import cats.Bifunctor
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import com.github.chenharryhua.nanjin.messages.kafka.instances.toJavaProducerRecordTransformer
import com.sksamuel.avro4s.*
import fs2.kafka.{Header, Headers, ProducerRecord}
import io.circe.{Decoder as JsonDecoder, Encoder as JsonEncoder, Json}
import io.scalaland.chimney.dsl.*
import monocle.Optional
import monocle.macros.Lenses
import monocle.std.option.some
import org.apache.kafka.clients.producer.ProducerRecord as KafkaProducerRecord
import shapeless.cachedImplicit

import scala.annotation.nowarn

@AvroNamespace("nj.spark.kafka")
@AvroName("NJProducerRecord")
@Lenses
final case class NJProducerRecord[K, V](
  topic: String,
  partition: Option[Int],
  offset: Option[Long], // for sort
  timestamp: Option[Long],
  key: Option[K],
  value: Option[V],
  headers: List[NJHeader]) {

  def withTopicName(name: TopicName): NJProducerRecord[K, V] =
    NJProducerRecord.topic.replace(name.value)(this)
  def withPartition(pt: Int): NJProducerRecord[K, V]  = NJProducerRecord.partition.replace(Some(pt))(this)
  def withTimestamp(ts: Long): NJProducerRecord[K, V] = NJProducerRecord.timestamp.replace(Some(ts))(this)
  def withKey(k: K): NJProducerRecord[K, V]           = NJProducerRecord.key.replace(Some(k))(this)
  def withValue(v: V): NJProducerRecord[K, V]         = NJProducerRecord.value.replace(Some(v))(this)

  def noPartition: NJProducerRecord[K, V] = NJProducerRecord.partition.replace(None)(this)
  def noTimestamp: NJProducerRecord[K, V] = NJProducerRecord.timestamp.replace(None)(this)
  def noHeaders: NJProducerRecord[K, V]   = NJProducerRecord.headers.replace(Nil)(this)

  def noMeta: NJProducerRecord[K, V] =
    NJProducerRecord
      .timestamp[K, V]
      .replace(None)
      .andThen(NJProducerRecord.partition[K, V].replace(None))
      .andThen(NJProducerRecord.headers.replace(Nil))(this)

  def modifyKey(f: K => K): NJProducerRecord[K, V] =
    NJProducerRecord.key.modify((_: Option[K]).map(f))(this)

  def modifyValue(f: V => V): NJProducerRecord[K, V] =
    NJProducerRecord.value.modify((_: Option[V]).map(f))(this)

  @SuppressWarnings(Array("AsInstanceOf"))
  def toProducerRecord: ProducerRecord[K, V] = {
    val hds = Headers.fromSeq(headers.map(h => Header(h.key, h.value)))
    val pr =
      ProducerRecord(topic, key.getOrElse(null.asInstanceOf[K]), value.getOrElse(null.asInstanceOf[V]))
        .withHeaders(hds)
    (partition, timestamp) match {
      case (None, None)       => pr
      case (Some(p), None)    => pr.withPartition(p)
      case (None, Some(t))    => pr.withTimestamp(t)
      case (Some(p), Some(t)) => pr.withPartition(p).withTimestamp(t)
    }
  }

  def toKafkaProducerRecord: KafkaProducerRecord[K, V] =
    toProducerRecord.transformInto[KafkaProducerRecord[K, V]]

  def asJson(implicit k: JsonEncoder[K], v: JsonEncoder[V]): Json =
    NJProducerRecord.jsonEncoder[K, V].apply(this)
}

object NJProducerRecord {
  def optionalKey[K, V]: Optional[NJProducerRecord[K, V], K] =
    NJProducerRecord.key[K, V].andThen(some[K])
  def optionalValue[K, V]: Optional[NJProducerRecord[K, V], V] =
    NJProducerRecord.value[K, V].andThen(some[V])

  def apply[K, V](pr: KafkaProducerRecord[Option[K], Option[V]]): NJProducerRecord[K, V] =
    NJProducerRecord(
      topic = pr.topic(),
      partition = Option(pr.partition.toInt),
      offset = None,
      timestamp = Option(pr.timestamp.toLong),
      key = pr.key,
      value = pr.value,
      headers = pr.headers().toArray.map(h => NJHeader(h.key(), h.value())).toList
    )

  def apply[K, V](topicName: TopicName, k: K, v: V): NJProducerRecord[K, V] =
    NJProducerRecord(topicName.value, None, None, None, Option(k), Option(v), Nil)

  def avroCodec[K, V](
    keyCodec: NJAvroCodec[K],
    valCodec: NJAvroCodec[V]): NJAvroCodec[NJProducerRecord[K, V]] = {
    @nowarn implicit val schemaForKey: SchemaFor[K] = keyCodec.schemaFor
    @nowarn implicit val schemaForVal: SchemaFor[V] = valCodec.schemaFor
    @nowarn implicit val keyDecoder: Decoder[K]     = keyCodec.avroDecoder
    @nowarn implicit val valDecoder: Decoder[V]     = valCodec.avroDecoder
    @nowarn implicit val keyEncoder: Encoder[K]     = keyCodec.avroEncoder
    @nowarn implicit val valEncoder: Encoder[V]     = valCodec.avroEncoder
    val s: SchemaFor[NJProducerRecord[K, V]]        = cachedImplicit
    val d: Decoder[NJProducerRecord[K, V]]          = cachedImplicit
    val e: Encoder[NJProducerRecord[K, V]]          = cachedImplicit
    NJAvroCodec[NJProducerRecord[K, V]](s, d.withSchema(s), e.withSchema(s))
  }

  implicit def jsonEncoder[K, V](implicit
    @nowarn jk: JsonEncoder[K],
    @nowarn jv: JsonEncoder[V]): JsonEncoder[NJProducerRecord[K, V]] =
    io.circe.generic.semiauto.deriveEncoder[NJProducerRecord[K, V]]

  implicit def jsonDecoder[K, V](implicit
    @nowarn jk: JsonDecoder[K],
    @nowarn jv: JsonDecoder[V]): JsonDecoder[NJProducerRecord[K, V]] =
    io.circe.generic.semiauto.deriveDecoder[NJProducerRecord[K, V]]

  implicit val bifunctorNJProducerRecord: Bifunctor[NJProducerRecord] =
    new Bifunctor[NJProducerRecord] {

      override def bimap[A, B, C, D](
        fab: NJProducerRecord[A, B])(f: A => C, g: B => D): NJProducerRecord[C, D] =
        fab.copy(key = fab.key.map(f), value = fab.value.map(g))
    }
}
