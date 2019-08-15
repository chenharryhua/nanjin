package com.github.chenharryhua.nanjin.kafka

import cats.implicits._
import cats.{Applicative, Bitraverse, Eval}
import fs2.kafka.{
  ConsumerRecord => Fs2ConsumerRecord,
  Headers        => Fs2Headers,
  ProducerRecord => Fs2ProducerRecord
}
import monocle.Iso
import monocle.macros.Lenses
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.Headers

@Lenses final case class NJProducerRecord[K, V](
  topic: String,
  key: K,
  value: V,
  partition: Option[Int],
  timestamp: Option[Long],
  headers: Option[Headers]) {
  def this(topic: String, key: K, value: V) = this(topic, key, value, None, None, None)
  def this(topic: String, key: K, value: V, timestamp: Long) =
    this(topic, key, value, None, Some(timestamp), None)
  def this(topic: String, key: K, value: V, partition: Int) =
    this(topic, key, value, Some(partition), None, None)
  def this(topic: String, key: K, value: V, partition: Int, timestamp: Long) =
    this(topic, key, value, Some(partition), Some(timestamp), None)

  def updateTimestamp(ts: Option[Long]): NJProducerRecord[K, V]       = copy(timestamp = ts)
  def updatePartition(partition: Option[Int]): NJProducerRecord[K, V] = copy(partition = partition)

  def producerRecord: ProducerRecord[K, V]       = NJProducerRecord.isoProducerRecord.get(this)
  def fs2ProducerRecord: Fs2ProducerRecord[K, V] = NJProducerRecord.isoFs2ProducerRecord.get(this)
}

object NJProducerRecord extends KafkaRecordBitraverse {

  def fromConsumerRecord[K, V](cr: ConsumerRecord[K, V]): NJProducerRecord[K, V] =
    NJProducerRecord(
      cr.topic(),
      cr.key(),
      cr.value(),
      Some(cr.partition()),
      Some(cr.timestamp()),
      Some(cr.headers()))

  def fromConsumerRecord[K, V](cr: Fs2ConsumerRecord[K, V]): NJProducerRecord[K, V] =
    NJProducerRecord(
      cr.topic,
      cr.key,
      cr.value,
      Some(cr.partition),
      cr.timestamp.createTime.orElse(cr.timestamp.logAppendTime),
      Some(cr.headers.asJava))

  implicit def isoProducerRecord[K, V]: Iso[NJProducerRecord[K, V], ProducerRecord[K, V]] =
    Iso[NJProducerRecord[K, V], ProducerRecord[K, V]](nj => {
      new ProducerRecord[K, V](
        nj.topic,
        nj.partition.map(new Integer(_)).orNull,
        nj.timestamp.map(new java.lang.Long(_)).orNull,
        nj.key,
        nj.value,
        nj.headers.orNull
      )
    })(
      pr =>
        NJProducerRecord[K, V](
          pr.topic(),
          pr.key(),
          pr.value(),
          Option(pr.partition()),
          Option(pr.timestamp()),
          Option(pr.headers())))

  implicit def isoFs2ProducerRecord[K, V]: Iso[NJProducerRecord[K, V], Fs2ProducerRecord[K, V]] =
    Iso[NJProducerRecord[K, V], Fs2ProducerRecord[K, V]](
      nj => {
        val fpr = Fs2ProducerRecord(nj.topic, nj.key, nj.value)
        val p   = nj.partition.fold(fpr)(fpr.withPartition)
        val t   = nj.timestamp.fold(p)(p.withTimestamp)
        nj.headers.fold(t)(h =>
          t.withHeaders(h.toArray.foldLeft(Fs2Headers.empty)((t, i) => t.append(i.key, i.value))))
      }
    )(
      fpr =>
        NJProducerRecord(
          fpr.topic,
          fpr.key,
          fpr.value,
          fpr.partition,
          fpr.timestamp,
          Some(fpr.headers.asJava)))

  implicit def bitraverseNJProducerRecord[K, V]: Bitraverse[NJProducerRecord] =
    new Bitraverse[NJProducerRecord] {
      override def bitraverse[G[_], A, B, C, D](fab: NJProducerRecord[A, B])(
        f: A => G[C],
        g: B => G[D])(implicit G: Applicative[G]): G[NJProducerRecord[C, D]] =
        isoProducerRecord.get(fab).bitraverse(f, g).map(isoProducerRecord.reverseGet)

      override def bifoldLeft[A, B, C](fab: NJProducerRecord[A, B], c: C)(
        f: (C, A) => C,
        g: (C, B) => C): C = isoProducerRecord.get(fab).bifoldLeft(c)(f, g)

      override def bifoldRight[A, B, C](fab: NJProducerRecord[A, B], c: Eval[C])(
        f: (A, Eval[C]) => Eval[C],
        g: (B, Eval[C]) => Eval[C]): Eval[C] = isoProducerRecord.get(fab).bifoldRight(c)(f, g)
    }
}
