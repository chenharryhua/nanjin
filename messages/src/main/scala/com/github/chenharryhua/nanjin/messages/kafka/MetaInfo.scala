package com.github.chenharryhua.nanjin.messages.kafka

import cats.implicits.catsSyntaxSemigroup
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
  serializedKeySize: Option[Int],
  serializedValueSize: Option[Int]
) {

  def size: Option[Int] = serializedKeySize |+| serializedValueSize

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
      partition = gr.get("partition").asInstanceOf[Int],
      offset = gr.get("offset").asInstanceOf[Long],
      timestamp = gr.get("timestamp").asInstanceOf[Long],
      timestampType = Option(gr.get("timestampType").asInstanceOf[Int]),
      serializedKeySize = Option(gr.get("serializedKeySize")).asInstanceOf[Option[Int]],
      serializedValueSize = Option(gr.get("serializedValueSize")).asInstanceOf[Option[Int]]
    )
  }

  def apply(rm: RecordMetadata): MetaInfo = {
    val keySize = {
      val k = rm.serializedKeySize()
      if (k == JavaConsumerRecord.NULL_SIZE) None else Some(k)
    }

    val valSize = {
      val v = rm.serializedValueSize()
      if (v == JavaConsumerRecord.NULL_SIZE) None else Some(v)
    }

    MetaInfo(
      topic = rm.topic(),
      partition = rm.partition(),
      offset = rm.offset(),
      timestamp = rm.timestamp(),
      timestampType = None,
      serializedKeySize = keySize,
      serializedValueSize = valSize
    )
  }
}

@JsonCodec
final case class ZonedMetaInfo(topic: String, partition: Int, offset: Long, timestamp: ZonedDateTime)
