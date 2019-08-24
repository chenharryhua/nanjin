package com.github.chenharryhua.nanjin.kafka

import cats.Show
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
  timestamp: Long,
  timestampType: TimestampType,
  checksum: Long,
  serializedKeySize: Int,
  serializedValueSize: Int,
  key: K,
  value: V,
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
        cr.timestamp(),
        cr.timestampType(),
        cr.checksum(): @silent,
        cr.serializedKeySize(),
        cr.serializedValueSize(),
        cr.key(),
        cr.value(),
        cr.headers(),
        cr.leaderEpoch().asScala.flatMap(Option(_))
      ))

  def from[K, V](d: ConsumerRecord[K, V]): NJConsumerRecord[K, V] = iso.reverseGet(d)

  implicit def showNJConsumerRecord[K: Show, V: Show]: Show[NJConsumerRecord[K, V]] =
    (t: NJConsumerRecord[K, V]) => iso.get(t).show
}
