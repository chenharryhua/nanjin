package com.github.chenharryhua.nanjin.messages.kafka

import cats.implicits.catsSyntaxSemigroup
import com.github.chenharryhua.nanjin.messages.ProtoConsumerRecord.ProtoConsumerRecord
import fs2.kafka.{CommittableConsumerRecord, ConsumerRecord as Fs2ConsumerRecord}
import io.circe.generic.JsonCodec
import io.scalaland.chimney.dsl.*
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecord as JavaConsumerRecord

import java.time.{Instant, LocalDateTime, ZoneId, ZonedDateTime}
import scala.util.Try

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

  def apply(pcr: ProtoConsumerRecord): CRMetaInfo =
    pcr.transformInto[CRMetaInfo]

  def apply(gr: GenericRecord): Try[CRMetaInfo] = Try {
    CRMetaInfo(
      topic = gr.get("topic").toString,
      partition = gr.get("partition").asInstanceOf[Int],
      offset = gr.get("offset").asInstanceOf[Long],
      timestamp = gr.get("timestamp").asInstanceOf[Long],
      timestampType = gr.get("timestampType").asInstanceOf[Int],
      serializedKeySize = Option(gr.get("serializedKeySize")).asInstanceOf[Option[Int]],
      serializedValueSize = Option(gr.get("serializedValueSize")).asInstanceOf[Option[Int]]
    )
  }
}

@JsonCodec
final case class ZonedCRMetaInfo(topic: String, partition: Int, offset: Long, timestamp: ZonedDateTime)
