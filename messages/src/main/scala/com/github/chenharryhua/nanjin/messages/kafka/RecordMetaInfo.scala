package com.github.chenharryhua.nanjin.messages.kafka

import cats.Show
import fs2.kafka.{
  CommittableConsumerRecord as Fs2CommittableConsumerRecord,
  ConsumerRecord as Fs2ConsumerRecord
}
import io.circe.generic.JsonCodec
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.typelevel.cats.time.instances.zoneddatetime

import java.time.{Instant, ZoneId, ZonedDateTime}

@JsonCodec
final case class RecordMetaInfo(topic: String, partition: Int, offset: Long, timestamp: ZonedDateTime)

object RecordMetaInfo extends zoneddatetime {
  implicit val showConsumerRecordMetaInfo: Show[RecordMetaInfo] =
    cats.derived.semiauto.show[RecordMetaInfo]

  def apply(cr: ConsumerRecord[?, ?], zoneId: ZoneId): RecordMetaInfo =
    RecordMetaInfo(
      topic = cr.topic(),
      partition = cr.partition(),
      offset = cr.offset(),
      timestamp = Instant.ofEpochMilli(cr.timestamp()).atZone(zoneId))

  def apply(cr: Fs2ConsumerRecord[?, ?], zoneId: ZoneId): RecordMetaInfo = {
    val ts: Long = cr.timestamp.createTime
      .orElse(cr.timestamp.logAppendTime)
      .orElse(cr.timestamp.unknownTime)
      .getOrElse(0L)
    RecordMetaInfo(
      topic = cr.topic,
      partition = cr.partition,
      offset = cr.offset,
      timestamp = Instant.ofEpochMilli(ts).atZone(zoneId))
  }

  def apply[F[_]](ccr: Fs2CommittableConsumerRecord[F, ?, ?], zoneId: ZoneId): RecordMetaInfo =
    apply(ccr.record, zoneId)

  def apply(rm: RecordMetadata, zoneId: ZoneId): RecordMetaInfo =
    RecordMetaInfo(
      rm.topic(),
      rm.partition(),
      rm.offset(),
      Instant.ofEpochMilli(rm.timestamp()).atZone(zoneId))
}
