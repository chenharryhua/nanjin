package com.github.chenharryhua.nanjin.messages.kafka

import cats.implicits.catsSyntaxSemigroup
import fs2.kafka.{CommittableConsumerRecord, ConsumerRecord as Fs2ConsumerRecord}
import io.circe.generic.JsonCodec
import io.scalaland.chimney.dsl.*
import org.apache.kafka.clients.consumer.ConsumerRecord as JavaConsumerRecord

import java.time.{Instant, LocalDateTime, ZoneId, ZonedDateTime}

@JsonCodec
final case class CRMetaInfo(
  topic: String,
  partition: Int,
  offset: Long,
  timestamp: Long,
  timestampType: Int,
  serializedKeySize: Option[Int],
  serializedValueSize: Option[Int]
) {

  def size: Option[Int] = serializedKeySize |+| serializedValueSize

  def localDateTime(zoneId: ZoneId): LocalDateTime =
    Instant.ofEpochMilli(timestamp).atZone(zoneId).toLocalDateTime

  def zoned(zoneId: ZoneId): ZonedCRMetaInfo =
    this
      .into[ZonedCRMetaInfo]
      .withFieldComputed(_.timestamp, cr => Instant.ofEpochMilli(cr.timestamp).atZone(zoneId))
      .transform
}

object CRMetaInfo {

  def apply[K, V](cr: NJConsumerRecord[K, V]): CRMetaInfo =
    cr.transformInto[CRMetaInfo]

  def apply[K, V](fcr: Fs2ConsumerRecord[K, V]): CRMetaInfo =
    apply(fcr.transformInto[NJConsumerRecord[K, V]])

  def apply[F[_], K, V](ccr: CommittableConsumerRecord[F, K, V]): CRMetaInfo =
    apply(ccr.record.transformInto[NJConsumerRecord[K, V]])

  def apply[K, V](jcr: JavaConsumerRecord[K, V]): CRMetaInfo =
    apply(jcr.transformInto[NJConsumerRecord[K, V]])
}

@JsonCodec
final case class ZonedCRMetaInfo(topic: String, partition: Int, offset: Long, timestamp: ZonedDateTime)
