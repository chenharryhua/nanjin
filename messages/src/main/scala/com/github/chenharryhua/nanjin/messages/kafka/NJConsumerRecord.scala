package com.github.chenharryhua.nanjin.messages.kafka

import alleycats.Empty
import cats.implicits._
import cats.kernel.{LowerBounded, PartialOrder}
import cats.{Bifunctor, Order, Show}
import com.github.chenharryhua.nanjin.datetime.NJTimestamp
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder => JsonDecoder, Encoder => JsonEncoder}
import monocle.macros.Lenses
import org.apache.kafka.clients.consumer.ConsumerRecord
import io.scalaland.chimney.dsl._

/**
  * compatible with spark kafka streaming
  * https://spark.apache.org/docs/3.0.0/structured-streaming-kafka-integration.html
  */
sealed trait NJConsumerRecordBase[K, V] { self =>
  def partition: Int
  def offset: Long
  def timestamp: Long
  def key: K
  def value: V
  def topic: String
  def timestampType: Int

  final def compare(other: NJConsumerRecordBase[K, V]): Int = {
    val ts = self.timestamp - other.timestamp
    val os = self.offset - other.offset
    if (ts != 0) ts.toInt
    else if (os != 0) os.toInt
    else self.partition - other.partition
  }
}

@Lenses final case class NJConsumerRecord[K, V](
  partition: Int,
  offset: Long,
  timestamp: Long,
  key: Option[K],
  value: Option[V],
  topic: String,
  timestampType: Int)
    extends NJConsumerRecordBase[Option[K], Option[V]] {
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

  def compulsoryK: Option[NJConsumerRecordK[K, V]] =
    key.map(k => this.into[NJConsumerRecordK[K, V]].withFieldConst(_.key, k).transform)

  def compulsoryV: Option[NJConsumerRecordV[K, V]] =
    value.map(v => this.into[NJConsumerRecordV[K, V]].withFieldConst(_.value, v).transform)

  def compulsoryKV: Option[NJConsumerRecordKV[K, V]] =
    (key, value).mapN {
      case (k, v) =>
        this
          .into[NJConsumerRecordKV[K, V]]
          .withFieldConst(_.key, k)
          .withFieldConst(_.value, v)
          .transform
    }
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

  implicit def emptyNJConsumerRecord[K, V]: Empty[NJConsumerRecord[K, V]] =
    new Empty[NJConsumerRecord[K, V]] {

      override val empty: NJConsumerRecord[K, V] =
        NJConsumerRecord(Int.MinValue, Long.MinValue, Long.MinValue, None, None, "", -1)
    }

  implicit def orderNJConsumerRecord[K, V]: Order[NJConsumerRecord[K, V]] =
    (x: NJConsumerRecord[K, V], y: NJConsumerRecord[K, V]) => x.compare(y)

  implicit def lowerBoundedNJConsumerRecord[K, V]: LowerBounded[NJConsumerRecord[K, V]] =
    new LowerBounded[NJConsumerRecord[K, V]] {

      override def partialOrder: PartialOrder[NJConsumerRecord[K, V]] = orderNJConsumerRecord
      override def minBound: NJConsumerRecord[K, V]                   = emptyNJConsumerRecord.empty
    }

  implicit def orderingNJConsumerRecord[K, V]: Ordering[NJConsumerRecord[K, V]] =
    orderNJConsumerRecord.toOrdering

  implicit def showNJConsumerRecord[K: Show, V: Show]: Show[NJConsumerRecord[K, V]] =
    nj =>
      s"CR(pt=${nj.partition},os=${nj.offset},ts=${NJTimestamp(
        nj.timestamp).utc},k=${nj.key.show},v=${nj.value.show})"
}

final case class NJConsumerRecordV[K, V](
  partition: Int,
  offset: Long,
  timestamp: Long,
  key: Option[K],
  value: V,
  topic: String,
  timestampType: Int)
    extends NJConsumerRecordBase[Option[K], V]

object NJConsumerRecordV {

  implicit val bifunctorNJConsumerRecordV: Bifunctor[NJConsumerRecordV] =
    new Bifunctor[NJConsumerRecordV] {

      override def bimap[A, B, C, D](
        fab: NJConsumerRecordV[A, B])(f: A => C, g: B => D): NJConsumerRecordV[C, D] =
        fab.copy(key = fab.key.map(f), value = g(fab.value))
    }

  implicit def orderNJConsumerRecordV[K, V]: Order[NJConsumerRecordV[K, V]] =
    (x: NJConsumerRecordV[K, V], y: NJConsumerRecordV[K, V]) => x.compare(y)

  implicit def orderingNJConsumerRecordV[K, V]: Ordering[NJConsumerRecordV[K, V]] =
    orderNJConsumerRecordV.toOrdering

}

final case class NJConsumerRecordK[K, V](
  partition: Int,
  offset: Long,
  timestamp: Long,
  key: K,
  value: Option[V],
  topic: String,
  timestampType: Int)
    extends NJConsumerRecordBase[K, Option[V]]

object NJConsumerRecordK {

  implicit val bifunctorNJConsumerRecordK: Bifunctor[NJConsumerRecordK] =
    new Bifunctor[NJConsumerRecordK] {

      override def bimap[A, B, C, D](
        fab: NJConsumerRecordK[A, B])(f: A => C, g: B => D): NJConsumerRecordK[C, D] =
        fab.copy(key = f(fab.key), value = fab.value.map(g))
    }

  implicit def orderNJConsumerRecordK[K, V]: Order[NJConsumerRecordK[K, V]] =
    (x: NJConsumerRecordK[K, V], y: NJConsumerRecordK[K, V]) => x.compare(y)

  implicit def orderingNJConsumerRecordK[K, V]: Ordering[NJConsumerRecordK[K, V]] =
    orderNJConsumerRecordK.toOrdering

}

final case class NJConsumerRecordKV[K, V](
  partition: Int,
  offset: Long,
  timestamp: Long,
  key: K,
  value: V,
  topic: String,
  timestampType: Int)
    extends NJConsumerRecordBase[K, V]

object NJConsumerRecordKV {

  implicit val bifunctorNJConsumerRecordKV: Bifunctor[NJConsumerRecordKV] =
    new Bifunctor[NJConsumerRecordKV] {

      override def bimap[A, B, C, D](
        fab: NJConsumerRecordKV[A, B])(f: A => C, g: B => D): NJConsumerRecordKV[C, D] =
        fab.copy(key = f(fab.key), value = g(fab.value))
    }

  implicit def orderNJConsumerRecordKV[K, V]: Order[NJConsumerRecordKV[K, V]] =
    (x: NJConsumerRecordKV[K, V], y: NJConsumerRecordKV[K, V]) => x.compare(y)

  implicit def orderingNJConsumerRecordKV[K, V]: Ordering[NJConsumerRecordKV[K, V]] =
    orderNJConsumerRecordKV.toOrdering
}
