package com.github.chenharryhua.nanjin.kafka

import cats.{Applicative, Bitraverse, Eval, Show}
import cats.implicits._
import com.github.ghik.silencer.silent
import monocle.Iso
import monocle.macros.Lenses
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.record.TimestampType

import scala.compat.java8.OptionConverters._

@Lenses final case class NJConsumerRecord[K, V](
  topic: String,
  partition: Int,
  offset: Long,
  key: K,
  value: V,
  timestamp: Long,
  timestampType: TimestampType,
  checksum: Long,
  serializedKeySize: Int,
  serializedValueSize: Int,
  headers: Headers,
  leaderEpoch: Option[Int])

object NJConsumerRecord extends ShowKafkaMessage {

  def iso[K, V]: Iso[NJConsumerRecord[K, V], ConsumerRecord[K, V]] =
    Iso[NJConsumerRecord[K, V], ConsumerRecord[K, V]](
      nj =>
        new ConsumerRecord[K, V](
          nj.topic,
          nj.partition,
          nj.offset,
          nj.timestamp,
          nj.timestampType,
          nj.checksum,
          nj.serializedKeySize,
          nj.serializedValueSize,
          nj.key,
          nj.value,
          nj.headers,
          nj.leaderEpoch.flatMap[Integer](Some(_)).asJava
        )
    )(cr =>
      NJConsumerRecord(
        cr.topic(),
        cr.partition(),
        cr.offset(),
        cr.key(),
        cr.value(),
        cr.timestamp(),
        cr.timestampType(),
        cr.checksum(): @silent,
        cr.serializedKeySize(),
        cr.serializedValueSize(),
        cr.headers(),
        cr.leaderEpoch().asScala.flatMap(Option(_))
      ))

  implicit final val bitraverseNJConsumerRecord: Bitraverse[NJConsumerRecord[?, ?]] =
    new Bitraverse[NJConsumerRecord] {
      override def bitraverse[G[_], A, B, C, D](fab: NJConsumerRecord[A, B])(
        f: A => G[C],
        g: B => G[D])(implicit evidence$1: Applicative[G]): G[NJConsumerRecord[C, D]] =
        iso.get(fab).bitraverse(f, g).map(iso.reverseGet)

      override def bifoldLeft[A, B, C](fab: NJConsumerRecord[A, B], c: C)(
        f: (C, A) => C,
        g: (C, B) => C): C = iso.get(fab).bifoldLeft(c)(f, g)

      override def bifoldRight[A, B, C](fab: NJConsumerRecord[A, B], c: Eval[C])(
        f: (A, Eval[C]) => Eval[C],
        g: (B, Eval[C]) => Eval[C]): Eval[C] = iso.get(fab).bifoldRight(c)(f, g)
    }

  def from[K, V](d: ConsumerRecord[K, V]): NJConsumerRecord[K, V] = iso.reverseGet(d)

  implicit def showNJConsumerRecord[K: Show, V: Show]: Show[NJConsumerRecord[K, V]] =
    (t: NJConsumerRecord[K, V]) => iso.get(t).show
}
