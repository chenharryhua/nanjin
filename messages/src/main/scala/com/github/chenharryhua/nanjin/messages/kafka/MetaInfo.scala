package com.github.chenharryhua.nanjin.messages.kafka

import com.github.chenharryhua.nanjin.messages.ProtoConsumerRecord.ProtoConsumerRecord
import fs2.kafka.{CommittableConsumerRecord, ConsumerRecord as Fs2ConsumerRecord}
import io.circe.generic.JsonCodec
import io.scalaland.chimney.dsl.*
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecord as JavaConsumerRecord
import org.apache.kafka.clients.producer.RecordMetadata

import java.time.{Instant, LocalDateTime, ZoneId, ZonedDateTime}
import scala.util.Try

@JsonCodec
final case class MetaInfo(
  topic: String,
  partition: Int,
  offset: Long,
  timestamp: Long,
  timestampType: Option[Int],
  serializedKeySize: Int,
  serializedValueSize: Int
) {

  def localDateTime(zoneId: ZoneId): LocalDateTime =
    Instant.ofEpochMilli(timestamp).atZone(zoneId).toLocalDateTime

  def zoned(zoneId: ZoneId): ZonedMetaInfo =
    this
      .into[ZonedMetaInfo]
      .withFieldComputed(_.timestamp, cr => Instant.ofEpochMilli(cr.timestamp).atZone(zoneId))
      .transform
}

object MetaInfo {

  def apply[K, V](cr: NJConsumerRecord[K, V]): MetaInfo =
    cr.transformInto[MetaInfo]

  def apply[K, V](fcr: Fs2ConsumerRecord[K, V]): MetaInfo =
    apply(fcr.transformInto[NJConsumerRecord[K, V]])

  def apply[F[_], K, V](ccr: CommittableConsumerRecord[F, K, V]): MetaInfo =
    apply(ccr.record.transformInto[NJConsumerRecord[K, V]])

  def apply[K, V](jcr: JavaConsumerRecord[K, V]): MetaInfo =
    apply(jcr.transformInto[NJConsumerRecord[K, V]])

  def apply(pcr: ProtoConsumerRecord): MetaInfo =
    pcr.transformInto[MetaInfo]

  def apply(gr: GenericRecord): Try[MetaInfo] = Try {
    MetaInfo(
      topic = gr.get("topic").toString,
      partition = gr.get("partition").asInstanceOf[Int], // scalafix:ok
      offset = gr.get("offset").asInstanceOf[Long], // scalafix:ok
      timestamp = gr.get("timestamp").asInstanceOf[Long], // scalafix:ok
      timestampType = Option(gr.get("timestampType").asInstanceOf[Int]), // scalafix:ok
      serializedKeySize = gr.get("serializedKeySize").asInstanceOf[Int], // scalafix:ok
      serializedValueSize = gr.get("serializedValueSize").asInstanceOf[Int] // scalafix:ok
    )

  }

  def apply(rm: RecordMetadata): MetaInfo =
    MetaInfo(
      topic = rm.topic(),
      partition = rm.partition(),
      offset = rm.offset(),
      timestamp = rm.timestamp(),
      timestampType = None,
      serializedKeySize = rm.serializedKeySize(),
      serializedValueSize = rm.serializedValueSize()
    )
}

@JsonCodec
final case class ZonedMetaInfo(topic: String, partition: Int, offset: Long, timestamp: ZonedDateTime)
