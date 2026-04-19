package com.github.chenharryhua.nanjin.kafka.record

import cats.Bitraverse
import cats.data.Cont
import cats.derived.derived
import cats.kernel.Eq
import com.github.chenharryhua.nanjin.kafka.TopicName
import com.sksamuel.avro4s.*
import fs2.kafka.{Header, Headers, ProducerRecord}
import io.circe.{Decoder as JsonDecoder, Encoder as JsonEncoder}
import io.scalaland.chimney.Transformer
import io.scalaland.chimney.dsl.into
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.ProducerRecord as JavaProducerRecord
import org.apache.kafka.common.header.Header as JavaHeader

import scala.jdk.CollectionConverters.*

@AvroDoc("kafka producer record, optional Key and optional Value")
@AvroNamespace("nanjin.kafka")
@AvroName("NJProducerRecord")
final case class NJProducerRecord[K, V](
  topic: String,
  partition: Option[Int],
  offset: Option[Long], // for sort
  timestamp: Option[Long],
  headers: List[NJHeader],
  key: Option[K],
  value: Option[V]
) derives Bitraverse, Eq {

  def withTopicName(name: TopicName): NJProducerRecord[K, V] = copy(topic = name.value)
  def withPartition(pt: Int): NJProducerRecord[K, V] = copy(partition = Some(pt))
  def withTimestamp(ts: Long): NJProducerRecord[K, V] = copy(timestamp = Some(ts))
  def withKey(k: K): NJProducerRecord[K, V] = copy(key = Some(k))
  def withValue(v: V): NJProducerRecord[K, V] = copy(value = Some(v))
  def withHeaders(headers: List[NJHeader]): NJProducerRecord[K, V] = copy(headers = headers)

  def noPartition: NJProducerRecord[K, V] = copy(partition = None)
  def noTimestamp: NJProducerRecord[K, V] = copy(timestamp = None)
  def noHeaders: NJProducerRecord[K, V] = copy(headers = Nil)

  def noKey: NJProducerRecord[K, V] = copy(key = None)
  def noValue: NJProducerRecord[K, V] = copy(value = None)

  def noMeta: NJProducerRecord[K, V] = copy(partition = None, timestamp = None, headers = Nil)

  def toProducerRecord(using Null <:< K, Null <:< V): ProducerRecord[K, V] =
    this.into[ProducerRecord[K, V]].transform
  def toJavaProducerRecord(using Null <:< K, Null <:< V): JavaProducerRecord[K, V] =
    this.into[JavaProducerRecord[K, V]].transform

  def toGenericRecord(using Encoder[K], Encoder[V], SchemaFor[K], SchemaFor[V]): GenericRecord = {
    val schema = summon[SchemaFor[NJProducerRecord[K, V]]].schema
    ToRecord[NJProducerRecord[K, V]](schema).to(this)
  }
}

object NJProducerRecord {

  def apply[K, V](pr: JavaProducerRecord[K, V]): NJProducerRecord[K, V] =
    pr.into[NJProducerRecord[K, V]].transform

  def apply[K, V](pr: ProducerRecord[K, V]): NJProducerRecord[K, V] =
    pr.into[NJProducerRecord[K, V]].transform

  def apply[K, V](topicName: TopicName, k: K, v: V): NJProducerRecord[K, V] =
    NJProducerRecord(
      topic = topicName.value,
      partition = None,
      offset = None,
      timestamp = None,
      headers = Nil,
      key = Option(k),
      value = Option(v))

  def schema(keySchema: Schema, valSchema: Schema): Schema = {
    class KEY
    class VAL
    given SchemaFor[KEY] = SchemaFor[KEY](keySchema)
    given SchemaFor[VAL] = SchemaFor[VAL](valSchema)
    SchemaFor[NJProducerRecord[KEY, VAL]].schema
  }

  given [K: JsonEncoder, V: JsonEncoder]: JsonEncoder[NJProducerRecord[K, V]] =
    io.circe.generic.semiauto.deriveEncoder[NJProducerRecord[K, V]]

  given [K: JsonDecoder, V: JsonDecoder]: JsonDecoder[NJProducerRecord[K, V]] =
    io.circe.generic.semiauto.deriveDecoder[NJProducerRecord[K, V]]

  given [K, V]: Transformer[JavaProducerRecord[K, V], NJProducerRecord[K, V]] =
    (src: JavaProducerRecord[K, V]) =>
      NJProducerRecord(
        topic = src.topic(),
        partition = Option(src.partition()).map(_.toInt),
        offset = None,
        timestamp = Option(src.timestamp()).map(_.toLong),
        key = Option(src.key()),
        value = Option(src.value()),
        headers = src.headers().toArray.map(_.into[NJHeader].transform).toList
      )

  given [K, V](using Null <:< K, Null <:< V): Transformer[NJProducerRecord[K, V], JavaProducerRecord[K, V]] =
    (src: NJProducerRecord[K, V]) =>
      new JavaProducerRecord[K, V](
        src.topic,
        src.partition.map(Integer.valueOf).orNull,
        src.timestamp.map(java.lang.Long.valueOf).orNull,
        src.key.orNull,
        src.value.orNull,
        src.headers.map(_.into[JavaHeader].transform).asJava
      )

  given [K, V]: Transformer[ProducerRecord[K, V], NJProducerRecord[K, V]] =
    (src: ProducerRecord[K, V]) =>
      NJProducerRecord(
        topic = src.topic,
        partition = src.partition,
        offset = None,
        timestamp = src.timestamp,
        key = Option(src.key),
        value = Option(src.value),
        headers = src.headers.toChain.map(_.into[NJHeader].transform).toList
      )

  given [K, V](using Null <:< K, Null <:< V): Transformer[NJProducerRecord[K, V], ProducerRecord[K, V]] =
    (src: NJProducerRecord[K, V]) =>
      Cont
        .pure(
          ProducerRecord[K, V](
            src.topic,
            src.key.orNull,
            src.value.orNull
          ).withHeaders(Headers.fromSeq(src.headers.map(_.into[Header].transform))))
        .map(pr => src.partition.fold(pr)(pr.withPartition))
        .map(pr => src.timestamp.fold(pr)(pr.withTimestamp))
        .eval
        .value
}
