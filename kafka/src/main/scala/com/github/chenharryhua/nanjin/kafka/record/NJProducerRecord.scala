package com.github.chenharryhua.nanjin.kafka.record

import cats.Bifunctor
import cats.data.Cont
import cats.kernel.Eq
import cats.syntax.eq.catsSyntaxEq
import com.sksamuel.avro4s.*
import fs2.kafka.{Header, Headers, ProducerRecord}
import io.circe.{Decoder as JsonDecoder, Encoder as JsonEncoder}
import io.scalaland.chimney.Transformer
import io.scalaland.chimney.dsl.into
import org.apache.avro.Schema
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
) {

  def withTopicName(name: String): NJProducerRecord[K, V] = copy(topic = name)
  def withPartition(pt: Int): NJProducerRecord[K, V] = copy(partition = Some(pt))
  def withTimestamp(ts: Long): NJProducerRecord[K, V] = copy(timestamp = Some(ts))
  def withKey(k: K): NJProducerRecord[K, V] = copy(key = Some(k))
  def withValue(v: V): NJProducerRecord[K, V] = copy(value = Some(v))
  def withHeaders(headers: List[NJHeader]): NJProducerRecord[K, V] = copy(headers = headers)

  def noPartition: NJProducerRecord[K, V] = copy(partition = None)
  def noTimestamp: NJProducerRecord[K, V] = copy(timestamp = None)
  def noHeaders: NJProducerRecord[K, V] = copy(headers = Nil)

  def noMeta: NJProducerRecord[K, V] = copy(partition = None, timestamp = None, headers = Nil)

  def toProducerRecord: ProducerRecord[K, V] = this.into[ProducerRecord[K, V]].transform
  def toJavaProducerRecord: JavaProducerRecord[K, V] = this.into[JavaProducerRecord[K, V]].transform
}

object NJProducerRecord {

  def apply[K, V](pr: JavaProducerRecord[K, V]): NJProducerRecord[K, V] =
    pr.into[NJProducerRecord[K, V]].transform

  def apply[K, V](pr: ProducerRecord[K, V]): NJProducerRecord[K, V] =
    pr.into[NJProducerRecord[K, V]].transform

  def apply[K, V](topicName: String, k: K, v: V): NJProducerRecord[K, V] =
    NJProducerRecord(
      topic = topicName,
      partition = None,
      offset = None,
      timestamp = None,
      headers = Nil,
      key = Option(k),
      value = Option(v))

  def schema(keySchema: Schema, valSchema: Schema): Schema = {
    class KEY
    class VAL
    implicit val schemaForKey: SchemaFor[KEY] = new SchemaFor[KEY] {
      override def schema: Schema = keySchema
    }

    implicit val schemaForVal: SchemaFor[VAL] = new SchemaFor[VAL] {
      override def schema: Schema = valSchema
    }
    SchemaFor[NJProducerRecord[KEY, VAL]].schema
  }

  implicit def encoderNJProducerRecord[K: JsonEncoder, V: JsonEncoder]: JsonEncoder[NJProducerRecord[K, V]] =
    io.circe.generic.semiauto.deriveEncoder[NJProducerRecord[K, V]]

  implicit def decoderNJProducerRecord[K: JsonDecoder, V: JsonDecoder]: JsonDecoder[NJProducerRecord[K, V]] =
    io.circe.generic.semiauto.deriveDecoder[NJProducerRecord[K, V]]

  implicit val bifunctorNJProducerRecord: Bifunctor[NJProducerRecord] =
    new Bifunctor[NJProducerRecord] {

      override def bimap[A, B, C, D](
        fab: NJProducerRecord[A, B])(f: A => C, g: B => D): NJProducerRecord[C, D] =
        fab.copy(key = fab.key.map(f), value = fab.value.map(g))
    }

  implicit def eqNJProducerRecord[K: Eq, V: Eq]: Eq[NJProducerRecord[K, V]] =
    Eq.instance { case (l, r) =>
      l.topic === r.topic &&
      l.partition === r.partition &&
      l.offset === r.offset &&
      l.timestamp === r.timestamp &&
      l.key === r.key &&
      l.value === r.value &&
      l.headers === r.headers
    }

  implicit def transformJavaNJ[K, V]: Transformer[JavaProducerRecord[K, V], NJProducerRecord[K, V]] =
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

  implicit def transformNJJava[K, V]: Transformer[NJProducerRecord[K, V], JavaProducerRecord[K, V]] =
    (src: NJProducerRecord[K, V]) =>
      new JavaProducerRecord[K, V](
        src.topic,
        src.partition.map(Integer.valueOf).orNull,
        src.timestamp.map(java.lang.Long.valueOf).orNull,

        src.key.getOrElse(null.asInstanceOf[K]), // scalafix:ok
        src.value.getOrElse(null.asInstanceOf[V]), // scalafix:ok

        src.headers.map(_.into[JavaHeader].transform).asJava
      )

  implicit def transformFs2NJ[K, V]: Transformer[ProducerRecord[K, V], NJProducerRecord[K, V]] =
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

  implicit def transformNJFs2[K, V]: Transformer[NJProducerRecord[K, V], ProducerRecord[K, V]] =
    (src: NJProducerRecord[K, V]) =>
      Cont
        .pure(
          ProducerRecord[K, V](
            src.topic,

            src.key.getOrElse(null.asInstanceOf[K]), // scalafix:ok
            src.value.getOrElse(null.asInstanceOf[V]) // scalafix:ok

          ).withHeaders(Headers.fromSeq(src.headers.map(_.into[Header].transform))))
        .map(pr => src.partition.fold(pr)(pr.withPartition))
        .map(pr => src.timestamp.fold(pr)(pr.withTimestamp))
        .eval
        .value
}
