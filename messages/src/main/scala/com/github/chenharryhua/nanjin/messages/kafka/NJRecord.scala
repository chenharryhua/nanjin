package com.github.chenharryhua.nanjin.messages.kafka

import alleycats.Empty
import cats.implicits._
import cats.kernel.{LowerBounded, PartialOrder}
import cats.{Bifunctor, Order, Show}
import com.github.chenharryhua.nanjin.datetime.NJTimestamp
import fs2.kafka.{ProducerRecord => Fs2ProducerRecord}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder => JsonDecoder, Encoder => JsonEncoder}
import monocle.macros.Lenses
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord

/**
  * compatible with spark kafka streaming
  * https://spark.apache.org/docs/3.0.0/structured-streaming-kafka-integration.html
  */
@Lenses final case class NJConsumerRecord[K, V](
  partition: Int,
  offset: Long,
  timestamp: Long,
  key: Option[K],
  value: Option[V],
  topic: String,
  timestampType: Int) {
  def njTimestamp: NJTimestamp = NJTimestamp(timestamp)

  def toNJProducerRecord: NJProducerRecord[K, V] =
    NJProducerRecord[K, V](Option(partition), Option(timestamp), key, value)

  def flatten[K2, V2](implicit
    evK: K <:< Option[K2],
    evV: V <:< Option[V2]
  ): NJConsumerRecord[K2, V2] =
    copy(key = key.flatten, value = value.flatten)

  def metaInfo: String =
    s"MetaInfo(topic=$topic,partition=$partition,offset=$offset,timestamp=${NJTimestamp(timestamp).utc})"
}

object NJConsumerRecord {

  def apply[K, V](cr: ConsumerRecord[Option[K], Option[V]]): NJConsumerRecord[K, V] =
    NJConsumerRecord(
      cr.partition,
      cr.offset,
      cr.timestamp,
      cr.key,
      cr.value,
      cr.topic,
      cr.timestampType.id)

  implicit def emptyNJConsumerRecord[K, V]: Empty[NJConsumerRecord[K, V]] =
    new Empty[NJConsumerRecord[K, V]] {

      override val empty: NJConsumerRecord[K, V] =
        NJConsumerRecord(Int.MinValue, Long.MinValue, Long.MinValue, None, None, "", -1)
    }

  implicit def jsonEncoderNJConsumerRecord[K: JsonEncoder, V: JsonEncoder]
    : JsonEncoder[NJConsumerRecord[K, V]] = deriveEncoder[NJConsumerRecord[K, V]]

  implicit def jsonDecoderNJConsumerRecord[K: JsonDecoder, V: JsonDecoder]
    : JsonDecoder[NJConsumerRecord[K, V]] = deriveDecoder[NJConsumerRecord[K, V]]

  implicit val bifunctorNJConsumerRecord: Bifunctor[NJConsumerRecord] =
    new Bifunctor[NJConsumerRecord] {

      override def bimap[A, B, C, D](
        fab: NJConsumerRecord[A, B])(f: A => C, g: B => D): NJConsumerRecord[C, D] =
        fab.copy(key = fab.key.map(f), value = fab.value.map(g))
    }

  implicit def orderNJConsumerRecord[K, V]: Order[NJConsumerRecord[K, V]] =
    (x: NJConsumerRecord[K, V], y: NJConsumerRecord[K, V]) => {
      val ts = x.timestamp - y.timestamp
      val os = x.offset - y.offset
      if (ts != 0) ts.toInt
      else if (os != 0) os.toInt
      else x.partition - y.partition
    }

  implicit def lowerBoundedNJConsumerRecord[K, V]: LowerBounded[NJConsumerRecord[K, V]] =
    new LowerBounded[NJConsumerRecord[K, V]] {
      override def partialOrder: PartialOrder[NJConsumerRecord[K, V]] = orderNJConsumerRecord

      override def minBound: NJConsumerRecord[K, V] = emptyNJConsumerRecord.empty
    }

  implicit def orderingNJConsumerRecord[K, V]: Ordering[NJConsumerRecord[K, V]] =
    orderNJConsumerRecord[K, V].toOrdering

  implicit def showNJConsumerRecord[K: Show, V: Show]: Show[NJConsumerRecord[K, V]] =
    nj =>
      s"CR(pt=${nj.partition},os=${nj.offset},ts=${NJTimestamp(
        nj.timestamp).utc},k=${nj.key.show},v=${nj.value.show})"
}

@Lenses final case class NJProducerRecord[K, V](
  partition: Option[Int],
  timestamp: Option[Long],
  key: Option[K],
  value: Option[V]) {

  def njTimestamp: Option[NJTimestamp] = timestamp.map(NJTimestamp(_))

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
