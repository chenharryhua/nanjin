package com.github.chenharryhua.nanjin.codec

import java.util.Optional

import cats.implicits._
import cats.{Applicative, Bitraverse, Eq, Eval}
import com.github.ghik.silencer.silent
import org.apache.kafka.clients.consumer.{ConsumerRecord, OffsetAndMetadata}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.header.{Header, Headers}

import scala.compat.java8.OptionConverters._

trait BitraverseKafkaRecord extends Serializable {
  implicit val eqArrayByte: Eq[Array[Byte]] =
    (x: Array[Byte], y: Array[Byte]) => x.sameElements(y)

  implicit val eqHeader: Eq[Header] = (x: Header, y: Header) =>
    (x.key() === y.key()) && (x.value() === y.value())

  implicit val eqHeaders: Eq[Headers] = (x: Headers, y: Headers) => {
    x.toArray.zip(y.toArray).forall { case (x, y) => x === y }
  }

  implicit val eqOptionalInteger: Eq[Optional[java.lang.Integer]] =
    (x: Optional[Integer], y: Optional[Integer]) =>
      x.asScala.flatMap(Option(_).map(_.toInt)) === y.asScala.flatMap(Option(_).map(_.toInt))

  implicit val eqTopicPartition: Eq[TopicPartition] =
    (x: TopicPartition, y: TopicPartition) => x.equals(y)

  implicit val eqOffsetAndMetadata: Eq[OffsetAndMetadata] =
    (x: OffsetAndMetadata, y: OffsetAndMetadata) => x.equals(y)

  implicit final def eqConsumerRecord[K: Eq, V: Eq]: Eq[ConsumerRecord[K, V]] =
    (x: ConsumerRecord[K, V], y: ConsumerRecord[K, V]) =>
      (x.topic() === y.topic) &&
        (x.partition() === y.partition()) &&
        (x.offset() === y.offset()) &&
        (x.timestamp() === y.timestamp()) &&
        (x.timestampType().id === y.timestampType().id) &&
        (x.serializedKeySize() === y.serializedKeySize()) &&
        (x.serializedValueSize() === y.serializedValueSize()) &&
        (x.key() === y.key()) &&
        (x.value() === y.value()) &&
        (x.headers() === y.headers()) &&
        (x.leaderEpoch() === y.leaderEpoch())

  implicit final def eqProducerRecord[K: Eq, V: Eq]: Eq[ProducerRecord[K, V]] =
    (x: ProducerRecord[K, V], y: ProducerRecord[K, V]) =>
      (x.topic() === y.topic()) &&
        x.partition().equals(y.partition()) &&
        x.timestamp().equals(y.timestamp()) &&
        (x.key() === y.key()) &&
        (x.value() === y.value()) &&
        (x.headers() === y.headers())

  implicit final val bitraverseConsumerRecord: Bitraverse[ConsumerRecord] =
    new Bitraverse[ConsumerRecord] {
      override def bimap[K1, V1, K2, V2](
        cr: ConsumerRecord[K1, V1])(k: K1 => K2, v: V1 => V2): ConsumerRecord[K2, V2] =
        new ConsumerRecord[K2, V2](
          cr.topic,
          cr.partition,
          cr.offset,
          cr.timestamp,
          cr.timestampType,
          cr.checksum: @silent,
          cr.serializedKeySize,
          cr.serializedValueSize,
          k(cr.key),
          v(cr.value),
          cr.headers,
          cr.leaderEpoch)

      override def bitraverse[G[_], A, B, C, D](fab: ConsumerRecord[A, B])(
        f: A => G[C],
        g: B => G[D])(implicit G: Applicative[G]): G[ConsumerRecord[C, D]] =
        G.map2(f(fab.key), g(fab.value))((k, v) => bimap(fab)(_ => k, _ => v))

      override def bifoldLeft[A, B, C](fab: ConsumerRecord[A, B], c: C)(
        f: (C, A) => C,
        g: (C, B) => C): C = g(f(c, fab.key), fab.value)

      override def bifoldRight[A, B, C](fab: ConsumerRecord[A, B], c: Eval[C])(
        f: (A, Eval[C]) => Eval[C],
        g: (B, Eval[C]) => Eval[C]): Eval[C] = g(fab.value, f(fab.key, c))
    }

  implicit final val bitraverseProducerRecord: Bitraverse[ProducerRecord] =
    new Bitraverse[ProducerRecord] {
      override def bimap[K1, V1, K2, V2](
        pr: ProducerRecord[K1, V1])(k: K1 => K2, v: V1 => V2): ProducerRecord[K2, V2] =
        new ProducerRecord[K2, V2](
          pr.topic,
          pr.partition,
          pr.timestamp,
          k(pr.key),
          v(pr.value),
          pr.headers)

      override def bitraverse[G[_], A, B, C, D](fab: ProducerRecord[A, B])(
        f: A => G[C],
        g: B => G[D])(implicit G: Applicative[G]): G[ProducerRecord[C, D]] =
        G.map2(f(fab.key), g(fab.value))((k, v) => bimap(fab)(_ => k, _ => v))

      override def bifoldLeft[A, B, C](fab: ProducerRecord[A, B], c: C)(
        f: (C, A) => C,
        g: (C, B) => C): C = g(f(c, fab.key), fab.value)

      override def bifoldRight[A, B, C](fab: ProducerRecord[A, B], c: Eval[C])(
        f: (A, Eval[C]) => Eval[C],
        g: (B, Eval[C]) => Eval[C]): Eval[C] = g(fab.value, f(fab.key, c))
    }
}
