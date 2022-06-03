package com.github.chenharryhua.nanjin.messages.kafka

import cats.Show
import fs2.kafka.CommittableConsumerRecord
import io.circe.generic.JsonCodec
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.typelevel.cats.time.instances.zoneddatetime

import java.time.{Instant, ZoneId, ZonedDateTime}

@JsonCodec
final case class ConsumerRecordMetaInfo(topic: String, partition: Int, offset: Long, timestamp: ZonedDateTime)
object ConsumerRecordMetaInfo extends zoneddatetime {
  implicit val showConsumerRecordMetaInfo: Show[ConsumerRecordMetaInfo] =
    cats.derived.semiauto.show[ConsumerRecordMetaInfo]

  def apply(cr: ConsumerRecord[?, ?], zoneId: ZoneId): ConsumerRecordMetaInfo = ConsumerRecordMetaInfo(
    topic = cr.topic(),
    partition = cr.partition(),
    offset = cr.offset(),
    timestamp = Instant.ofEpochMilli(cr.timestamp()).atZone(zoneId))

  def apply[F[_]](ccr: CommittableConsumerRecord[F, ?, ?], zoneId: ZoneId): ConsumerRecordMetaInfo = {
    val ts: Long = ccr.record.timestamp.createTime
      .orElse(ccr.record.timestamp.logAppendTime)
      .orElse(ccr.record.timestamp.unknownTime)
      .getOrElse(0L)
    ConsumerRecordMetaInfo(
      topic = ccr.record.topic,
      partition = ccr.record.partition,
      offset = ccr.record.offset,
      timestamp = Instant.ofEpochMilli(ts).atZone(zoneId))
  }
}
