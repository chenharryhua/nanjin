package com.github.chenharryhua.nanjin.messages.kafka

import alleycats.Empty
import cats.implicits._
import cats.{Bifunctor, Show}
import fs2.kafka.{ProducerRecord => Fs2ProducerRecord}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder => JsonDecoder, Encoder => JsonEncoder}
import monocle.macros.Lenses
import org.apache.kafka.clients.producer.ProducerRecord

@Lenses final case class NJProducerRecord[K, V](
  partition: Option[Int],
  timestamp: Option[Long],
  key: Option[K],
  value: Option[V]) {

  def newPartition(pt: Int): NJProducerRecord[K, V] =
    NJProducerRecord.partition.set(Some(pt))(this)

  def newTimestamp(ts: Long): NJProducerRecord[K, V] =
    NJProducerRecord.timestamp.set(Some(ts))(this)

  def newKey(k: K): NJProducerRecord[K, V] =
    NJProducerRecord.key.set(Some(k))(this)

  def newValue(v: V): NJProducerRecord[K, V] =
    NJProducerRecord.value.set(Some(v))(this)

  def noPartition: NJProducerRecord[K, V] =
    NJProducerRecord.partition.set(None)(this)

  def noTimestamp: NJProducerRecord[K, V] =
    NJProducerRecord.timestamp.set(None)(this)

  def noMeta: NJProducerRecord[K, V] =
    NJProducerRecord
      .timestamp[K, V]
      .set(None)
      .andThen(NJProducerRecord.partition[K, V].set(None))(this)

  def modifyKey(f: K => K): NJProducerRecord[K, V] =
    NJProducerRecord.key.modify((_: Option[K]).map(f))(this)

  def modifyValue(f: V => V): NJProducerRecord[K, V] =
    NJProducerRecord.value.modify((_: Option[V]).map(f))(this)

  @SuppressWarnings(Array("AsInstanceOf"))
  def toFs2ProducerRecord(topicName: String): Fs2ProducerRecord[K, V] = {
    val pr = Fs2ProducerRecord(
      topicName,
      key.getOrElse(null.asInstanceOf[K]),
      value.getOrElse(null.asInstanceOf[V]))
    val pt = partition.fold(pr)(pr.withPartition)
    timestamp.fold(pt)(pt.withTimestamp)
  }
}

object NJProducerRecord {

  def apply[K, V](pr: ProducerRecord[Option[K], Option[V]]): NJProducerRecord[K, V] =
    NJProducerRecord(Option(pr.partition), Option(pr.timestamp), pr.key, pr.value)

  def apply[K, V](k: K, v: V): NJProducerRecord[K, V] =
    NJProducerRecord(None, None, Option(k), Option(v))

  def apply[K, V](v: V): NJProducerRecord[K, V] =
    NJProducerRecord(None, None, None, Option(v))

  implicit def emptyNJProducerRecord[K, V]: Empty[NJProducerRecord[K, V]] =
    new Empty[NJProducerRecord[K, V]] {
      override val empty: NJProducerRecord[K, V] = NJProducerRecord(None, None, None, None)
    }

  implicit def jsonEncoderNJProducerRecord[K: JsonEncoder, V: JsonEncoder]
    : JsonEncoder[NJProducerRecord[K, V]] =
    deriveEncoder[NJProducerRecord[K, V]]

  implicit def jsonDecoderNJProducerRecord[K: JsonDecoder, V: JsonDecoder]
    : JsonDecoder[NJProducerRecord[K, V]] =
    deriveDecoder[NJProducerRecord[K, V]]

  implicit val bifunctorNJProducerRecord: Bifunctor[NJProducerRecord] =
    new Bifunctor[NJProducerRecord] {

      override def bimap[A, B, C, D](
        fab: NJProducerRecord[A, B])(f: A => C, g: B => D): NJProducerRecord[C, D] =
        fab.copy(key = fab.key.map(f), value = fab.value.map(g))
    }

  implicit def showNJProducerRecord[K: Show, V: Show]: Show[NJProducerRecord[K, V]] =
    nj => s"PR(k=${nj.key.show},v=${nj.value.show})"
}
