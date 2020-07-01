package com.github.chenharryhua.nanjin.messages.kafka

import alleycats.Empty
import cats.implicits._
import cats.kernel.{LowerBounded, PartialOrder}
import cats.{Applicative, Bifunctor, Bitraverse, Eval, Order, Show}
import com.github.chenharryhua.nanjin.datetime.NJTimestamp
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder => JsonDecoder, Encoder => JsonEncoder}
import io.scalaland.chimney.dsl._
import monocle.macros.Lenses
import org.apache.kafka.clients.consumer.ConsumerRecord

/**
  * compatible with spark kafka streaming
  * https://spark.apache.org/docs/3.0.0/structured-streaming-kafka-integration.html
  */
sealed trait NJConsumerRecordBase[K, V] { self =>
  val partition: Int
  val offset: Long
  val timestamp: Long
  val key: K
  val value: V
  val topic: String
  val timestampType: Int

  final def compare(other: NJConsumerRecordBase[K, V]): Int = {
    val ts = self.timestamp - other.timestamp
    val os = self.offset - other.offset
    if (ts != 0) ts.toInt
    else if (os != 0) os.toInt
    else self.partition - other.partition
  }

  final def njTimestamp: NJTimestamp = NJTimestamp(timestamp)

  final def metaInfo: String =
    s"Meta(topic=$topic,partition=$partition,offset=$offset,ts=${njTimestamp.utc})"

  final def show(implicit k: Show[K], v: Show[V]): String =
    s"CR($metaInfo,key=${key.show},value=${value.show})"

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

  def toNJProducerRecord: NJProducerRecord[K, V] =
    NJProducerRecord[K, V](Option(partition), Option(timestamp), key, value)

  def flatten[K2, V2](implicit
    evK: K <:< Option[K2],
    evV: V <:< Option[V2]
  ): NJConsumerRecord[K2, V2] =
    copy(key = key.flatten, value = value.flatten)

  def compulsoryK: Option[CompulsoryK[K, V]] =
    key.map(k => this.into[CompulsoryK[K, V]].withFieldConst(_.key, k).transform)

  def compulsoryV: Option[CompulsoryV[K, V]] =
    value.map(v => this.into[CompulsoryV[K, V]].withFieldConst(_.value, v).transform)

  def compulsoryKV: Option[CompulsoryKV[K, V]] =
    (key, value).mapN {
      case (k, v) =>
        this.into[CompulsoryKV[K, V]].withFieldConst(_.key, k).withFieldConst(_.value, v).transform
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
}

final case class CompulsoryV[K, V](
  partition: Int,
  offset: Long,
  timestamp: Long,
  key: Option[K],
  value: V,
  topic: String,
  timestampType: Int)
    extends NJConsumerRecordBase[Option[K], V] {

  def compulsoryK: Option[CompulsoryKV[K, V]] =
    key.map(k => this.into[CompulsoryKV[K, V]].withFieldConst(_.key, k).transform)

}

object CompulsoryV {

  implicit val bifunctorCompulsoryV: Bifunctor[CompulsoryV] =
    new Bifunctor[CompulsoryV] {

      override def bimap[A, B, C, D](
        fab: CompulsoryV[A, B])(f: A => C, g: B => D): CompulsoryV[C, D] =
        fab.copy(key = fab.key.map(f), value = g(fab.value))
    }

  implicit def orderCompulsoryV[K, V]: Order[CompulsoryV[K, V]] =
    (x: CompulsoryV[K, V], y: CompulsoryV[K, V]) => x.compare(y)

  implicit def orderingCompulsoryV[K, V]: Ordering[CompulsoryV[K, V]] =
    orderCompulsoryV.toOrdering

}

final case class CompulsoryK[K, V](
  partition: Int,
  offset: Long,
  timestamp: Long,
  key: K,
  value: Option[V],
  topic: String,
  timestampType: Int)
    extends NJConsumerRecordBase[K, Option[V]] {

  def compulsoryV: Option[CompulsoryKV[K, V]] =
    value.map(v => this.into[CompulsoryKV[K, V]].withFieldConst(_.value, v).transform)

}

object CompulsoryK {

  implicit val bifunctorCompulsoryK: Bifunctor[CompulsoryK] =
    new Bifunctor[CompulsoryK] {

      override def bimap[A, B, C, D](
        fab: CompulsoryK[A, B])(f: A => C, g: B => D): CompulsoryK[C, D] =
        fab.copy(key = f(fab.key), value = fab.value.map(g))
    }

  implicit def orderCompulsoryK[K, V]: Order[CompulsoryK[K, V]] =
    (x: CompulsoryK[K, V], y: CompulsoryK[K, V]) => x.compare(y)

  implicit def orderingCompulsoryK[K, V]: Ordering[CompulsoryK[K, V]] =
    orderCompulsoryK.toOrdering

}

final case class CompulsoryKV[K, V](
  partition: Int,
  offset: Long,
  timestamp: Long,
  key: K,
  value: V,
  topic: String,
  timestampType: Int)
    extends NJConsumerRecordBase[K, V]

object CompulsoryKV {

  implicit val bifunctorCompulsoryKV: Bitraverse[CompulsoryKV] =
    new Bitraverse[CompulsoryKV] {

      override def bimap[A, B, C, D](
        fab: CompulsoryKV[A, B])(f: A => C, g: B => D): CompulsoryKV[C, D] =
        fab.copy(key = f(fab.key), value = g(fab.value))

      override def bitraverse[G[_], A, B, C, D](
        fab: CompulsoryKV[A, B])(f: A => G[C], g: B => G[D])(implicit
        G: Applicative[G]): G[CompulsoryKV[C, D]] =
        G.map2(f(fab.key), g(fab.value))((k, v) => bimap(fab)(_ => k, _ => v))

      override def bifoldLeft[A, B, C](fab: CompulsoryKV[A, B], c: C)(
        f: (C, A) => C,
        g: (C, B) => C): C = g(f(c, fab.key), fab.value)

      override def bifoldRight[A, B, C](fab: CompulsoryKV[A, B], c: Eval[C])(
        f: (A, Eval[C]) => Eval[C],
        g: (B, Eval[C]) => Eval[C]): Eval[C] = g(fab.value, f(fab.key, c))
    }

  implicit def orderCompulsoryKV[K, V]: Order[CompulsoryKV[K, V]] =
    (x: CompulsoryKV[K, V], y: CompulsoryKV[K, V]) => x.compare(y)

  implicit def orderingNJConsumerRecordKV[K, V]: Ordering[CompulsoryKV[K, V]] =
    orderCompulsoryKV.toOrdering
}
