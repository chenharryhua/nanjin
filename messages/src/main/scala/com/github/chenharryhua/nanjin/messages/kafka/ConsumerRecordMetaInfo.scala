package com.github.chenharryhua.nanjin.messages.kafka

import cats.Show
import fs2.kafka.{
  CommittableConsumerRecord as Fs2CommittableConsumerRecord,
  ConsumerRecord as Fs2ConsumerRecord
}
import io.circe.generic.JsonCodec
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.typelevel.cats.time.instances.zoneddatetime

import java.time.{Instant, ZoneId, ZonedDateTime}

@JsonCodec
final case class ConsumerRecordMetaInfo(topic: String, partition: Int, offset: Long, timestamp: ZonedDateTime)
object ConsumerRecordMetaInfo extends zoneddatetime {
  implicit val showConsumerRecordMetaInfo: Show[ConsumerRecordMetaInfo] =
    cats.derived.semiauto.show[ConsumerRecordMetaInfo]

  def apply(cr: ConsumerRecord[?, ?], zoneId: ZoneId): ConsumerRecordMetaInfo =
    ConsumerRecordMetaInfo(
      topic = cr.topic(),
      partition = cr.partition(),
      offset = cr.offset(),
      timestamp = Instant.ofEpochMilli(cr.timestamp()).atZone(zoneId))

  def apply(cr: Fs2ConsumerRecord[?, ?], zoneId: ZoneId): ConsumerRecordMetaInfo = {
    val ts: Long = cr.timestamp.createTime
      .orElse(cr.timestamp.logAppendTime)
      .orElse(cr.timestamp.unknownTime)
      .getOrElse(0L)
    ConsumerRecordMetaInfo(
      topic = cr.topic,
      partition = cr.partition,
      offset = cr.offset,
      timestamp = Instant.ofEpochMilli(ts).atZone(zoneId))
  }

  def apply[F[_]](ccr: Fs2CommittableConsumerRecord[F, ?, ?], zoneId: ZoneId): ConsumerRecordMetaInfo =
    apply(ccr.record, zoneId)
}
