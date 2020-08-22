package com.github.chenharryhua.nanjin.messages.kafka

import java.time.Instant

import alleycats.Empty
import cats.implicits._
import cats.kernel.{LowerBounded, PartialOrder}
import cats.{Applicative, Bifunctor, Bitraverse, Eval, Order, Show}
import com.sksamuel.avro4s.AvroDoc
import io.scalaland.chimney.dsl._
import monocle.macros.Lenses
import org.apache.kafka.clients.consumer.ConsumerRecord

/**
  * compatible with spark kafka streaming
  * https://spark.apache.org/docs/3.0.0/structured-streaming-kafka-integration.html
  */
sealed trait NJConsumerRecord[K, V] {
  self =>
  val partition: Int
  val offset: Long
  val timestamp: Long
  val key: K
  val value: V
  val topic: String
  val timestampType: Int

  final def compare(other: NJConsumerRecord[K, V]): Int = {
    val ts = self.timestamp.compareTo(other.timestamp)
    val os = self.offset.compareTo(other.offset)
    if (ts != 0) ts
    else if (os != 0) os
    else self.partition.compareTo(other.partition)
  }

  final def metaInfo: String =
    s"Meta(topic=$topic,partition=$partition,offset=$offset,ts=${Instant.ofEpochMilli(timestamp)})"

  final def display(k: Show[K], v: Show[V]): String =
    s"CR($metaInfo,key=${k.show(key)},value=${v.show(value)})"

}

object NJConsumerRecord {

  implicit def showNJConsumerRecord[A, K, V](implicit
    ev: A <:< NJConsumerRecord[K, V],
    k: Show[K],
    v: Show[V]): Show[A] = _.display(k, v)

  implicit def orderNJConsumerRecord[A, K, V](implicit
    ev: A <:< NJConsumerRecord[K, V]
  ): Order[A] = (x: A, y: A) => x.compare(y)
}

@Lenses @AvroDoc("kafka record, optional Key and Value")
final case class OptionalKV[K, V](
  @AvroDoc("kafka partition") partition: Int,
  @AvroDoc("kafka offset") offset: Long,
  @AvroDoc("kafka timestamp in millisecond") timestamp: Long,
  key: Option[K],
  value: Option[V],
  @AvroDoc("kafka topic") topic: String,
  @AvroDoc("kafka timestamp type") timestampType: Int)
    extends NJConsumerRecord[Option[K], Option[V]] {

  def flatten[K2, V2](implicit
    evK: K <:< Option[K2],
    evV: V <:< Option[V2]
  ): OptionalKV[K2, V2] =
    copy(key = key.flatten, value = value.flatten)

  def toNJProducerRecord: NJProducerRecord[K, V] =
    NJProducerRecord[K, V](Some(partition), Some(timestamp), key, value)

  def toCompulsoryK: Option[CompulsoryK[K, V]] =
    key.map(k => this.into[CompulsoryK[K, V]].withFieldConst(_.key, k).transform)

  def toCompulsoryV: Option[CompulsoryV[K, V]] =
    value.map(v => this.into[CompulsoryV[K, V]].withFieldConst(_.value, v).transform)

  def toCompulsoryKV: Option[CompulsoryKV[K, V]] =
    (key, value).mapN {
      case (k, v) =>
        this.into[CompulsoryKV[K, V]].withFieldConst(_.key, k).withFieldConst(_.value, v).transform
    }
}

object OptionalKV {

  def apply[K, V](cr: ConsumerRecord[Option[K], Option[V]]): OptionalKV[K, V] =
    OptionalKV(
      cr.partition,
      cr.offset,
      cr.timestamp,
      cr.key,
      cr.value,
      cr.topic,
      cr.timestampType.id)

  implicit val bifunctorOptionalKV: Bifunctor[OptionalKV] =
    new Bifunctor[OptionalKV] {

      override def bimap[A, B, C, D](
        fab: OptionalKV[A, B])(f: A => C, g: B => D): OptionalKV[C, D] =
        fab.copy(key = fab.key.map(f), value = fab.value.map(g))
    }

  implicit def emptyOptionalKV[K, V]: Empty[OptionalKV[K, V]] =
    new Empty[OptionalKV[K, V]] {

      override val empty: OptionalKV[K, V] =
        OptionalKV(Int.MinValue, Long.MinValue, Long.MinValue, None, None, "", -1)
    }

  implicit def lowerBoundedOptionalKV[K, V]: LowerBounded[OptionalKV[K, V]] =
    new LowerBounded[OptionalKV[K, V]] {

      override def partialOrder: PartialOrder[OptionalKV[K, V]] =
        Order[OptionalKV[K, V]]

      override def minBound: OptionalKV[K, V] =
        emptyOptionalKV.empty
    }

}

@AvroDoc("kafka record, optional Key and compulsory Value")
final case class CompulsoryV[K, V](
  partition: Int,
  offset: Long,
  timestamp: Long,
  key: Option[K],
  value: V,
  topic: String,
  timestampType: Int)
    extends NJConsumerRecord[Option[K], V] {

  def toNJProducerRecord: NJProducerRecord[K, V] =
    NJProducerRecord[K, V](Some(partition), Some(timestamp), key, Some(value))

  def toCompulsoryKV: Option[CompulsoryKV[K, V]] =
    key.map(k => this.into[CompulsoryKV[K, V]].withFieldConst(_.key, k).transform)

  def toOptionalKV: OptionalKV[K, V] =
    this.into[OptionalKV[K, V]].withFieldConst(_.value, Some(value)).transform

}

object CompulsoryV {

  implicit val bifunctorCompulsoryV: Bifunctor[CompulsoryV] =
    new Bifunctor[CompulsoryV] {

      override def bimap[A, B, C, D](
        fab: CompulsoryV[A, B])(f: A => C, g: B => D): CompulsoryV[C, D] =
        fab.copy(key = fab.key.map(f), value = g(fab.value))
    }
}

@AvroDoc("kafka record, compulsory Key and optional Value")
final case class CompulsoryK[K, V](
  partition: Int,
  offset: Long,
  timestamp: Long,
  key: K,
  value: Option[V],
  topic: String,
  timestampType: Int)
    extends NJConsumerRecord[K, Option[V]] {

  def toNJProducerRecord: NJProducerRecord[K, V] =
    NJProducerRecord[K, V](Some(partition), Some(timestamp), Some(key), value)

  def toCompulsoryKV: Option[CompulsoryKV[K, V]] =
    value.map(v => this.into[CompulsoryKV[K, V]].withFieldConst(_.value, v).transform)

  def toOptionalKV: OptionalKV[K, V] =
    this.into[OptionalKV[K, V]].withFieldConst(_.key, Some(key)).transform

}

object CompulsoryK {

  implicit val bifunctorCompulsoryK: Bifunctor[CompulsoryK] =
    new Bifunctor[CompulsoryK] {

      override def bimap[A, B, C, D](
        fab: CompulsoryK[A, B])(f: A => C, g: B => D): CompulsoryK[C, D] =
        fab.copy(key = f(fab.key), value = fab.value.map(g))
    }
}

@AvroDoc("kafka record, compulsory Key and Value")
final case class CompulsoryKV[K, V](
  partition: Int,
  offset: Long,
  timestamp: Long,
  key: K,
  value: V,
  topic: String,
  timestampType: Int)
    extends NJConsumerRecord[K, V] {

  def toNJProducerRecord: NJProducerRecord[K, V] =
    NJProducerRecord[K, V](Some(partition), Some(timestamp), Some(key), Some(value))

  def toOptionalKV: OptionalKV[K, V] =
    this
      .into[OptionalKV[K, V]]
      .withFieldConst(_.key, Some(key))
      .withFieldConst(_.value, Some(value))
      .transform

}

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
