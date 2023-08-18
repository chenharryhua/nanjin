package com.github.chenharryhua.nanjin.messages.kafka

import cats.Bifunctor
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import com.github.chenharryhua.nanjin.messages.kafka.instances.toJavaProducerRecordTransformer
import com.sksamuel.avro4s.*
import fs2.kafka.{Header as Fs2Header, Headers, ProducerRecord}
import io.scalaland.chimney.dsl.*
import org.apache.kafka.clients.producer.ProducerRecord as KafkaProducerRecord
import shapeless.cachedImplicit

import scala.annotation.nowarn

@AvroNamespace("nj.spark.kafka")
@AvroName("NJProducerRecord")
final case class NJProducerRecord[K, V](
  topic: String,
  partition: Option[Int],
  offset: Option[Long], // for sort
  timestamp: Option[Long],
  key: Option[K],
  value: Option[V],
  headers: List[Header]) {

  def withTopicName(name: TopicName): NJProducerRecord[K, V]     = copy(topic = name.value)
  def withPartition(pt: Int): NJProducerRecord[K, V]             = copy(partition = Some(pt))
  def withTimestamp(ts: Long): NJProducerRecord[K, V]            = copy(timestamp = Some(ts))
  def withKey(k: K): NJProducerRecord[K, V]                      = copy(key = Some(k))
  def withValue(v: V): NJProducerRecord[K, V]                    = copy(value = Some(v))
  def withHeaders(headers: List[Header]): NJProducerRecord[K, V] = copy(headers = headers)

  def noPartition: NJProducerRecord[K, V] = copy(partition = None)
  def noTimestamp: NJProducerRecord[K, V] = copy(timestamp = None)
  def noHeaders: NJProducerRecord[K, V]   = copy(headers = Nil)

  def noMeta: NJProducerRecord[K, V] = copy(partition = None, timestamp = None, headers = Nil)

  @SuppressWarnings(Array("AsInstanceOf"))
  def toProducerRecord: ProducerRecord[K, V] = {
    val hds = Headers.fromSeq(headers.map(h => Fs2Header(h.key, h.value)))
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
}

object NJProducerRecord {

  def apply[K, V](pr: KafkaProducerRecord[Option[K], Option[V]]): NJProducerRecord[K, V] =
    NJProducerRecord(
      topic = pr.topic(),
      partition = Option(pr.partition.toInt),
      offset = None,
      timestamp = Option(pr.timestamp.toLong),
      key = pr.key,
      value = pr.value,
      headers = pr.headers().toArray.map(h => Header(h.key(), h.value())).toList
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

  implicit val bifunctorNJProducerRecord: Bifunctor[NJProducerRecord] =
    new Bifunctor[NJProducerRecord] {

      override def bimap[A, B, C, D](
        fab: NJProducerRecord[A, B])(f: A => C, g: B => D): NJProducerRecord[C, D] =
        fab.copy(key = fab.key.map(f), value = fab.value.map(g))
    }
}
