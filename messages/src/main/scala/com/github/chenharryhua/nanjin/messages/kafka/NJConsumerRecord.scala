package com.github.chenharryhua.nanjin.messages.kafka

import alleycats.Empty
import cats.implicits._
import cats.kernel.{LowerBounded, PartialOrder}
import cats.{Applicative, Bifunctor, Bitraverse, Eval, Order, Show}
import com.github.chenharryhua.nanjin.datetime.NJTimestamp
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

  final def show(k: Show[K], v: Show[V]): String =
    s"CR($metaInfo,key=${k.show(key)},value=${v.show(value)})"

}

object NJConsumerRecordBase {

  implicit def showNJConsumerRecordBase[A, K, V](implicit
    a: A <:< NJConsumerRecordBase[K, V],
    k: Show[K],
    v: Show[V]): Show[A] = _.show(k, v)

  implicit def orderNJConsumerRecordBase[A, K, V](implicit
    a: A <:< NJConsumerRecordBase[K, V]
  ): Order[A] = (x: A, y: A) => x.compare(y)

  implicit def orderingNJConsumerRecordBase[A, K, V](implicit
    a: A <:< NJConsumerRecordBase[K, V]): Ordering[A] =
    orderNJConsumerRecordBase[A, K, V].toOrdering
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

  implicit def lowerBoundedNJConsumerRecord[K, V]: LowerBounded[NJConsumerRecord[K, V]] =
    new LowerBounded[NJConsumerRecord[K, V]] {

      override def partialOrder: PartialOrder[NJConsumerRecord[K, V]] =
        Order[NJConsumerRecord[K, V]]

      override def minBound: NJConsumerRecord[K, V] =
        emptyNJConsumerRecord.empty
    }

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
}
