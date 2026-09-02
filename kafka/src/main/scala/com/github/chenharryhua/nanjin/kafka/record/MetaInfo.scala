package com.github.chenharryhua.nanjin.kafka.record

import fs2.kafka.{CommittableConsumerRecord, ConsumerRecord as Fs2ConsumerRecord}
import io.circe.Codec
import io.scalaland.chimney.dsl.into
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecord as JavaConsumerRecord
import org.apache.kafka.clients.producer.RecordMetadata

import cats.syntax.traverse.given

import java.time.{Instant, LocalDateTime, ZoneId, ZonedDateTime}
import scala.util.{Failure, Success, Try}

/** Kafka record metadata, decoupled from the record's key/value payload.
  *
  * Captures the coordinates and sizing of a single Kafka record. It is derived from the various record shapes
  * this library works with (fs2-kafka, the Java client, an Avro `GenericRecord`, or a producer's
  * `RecordMetadata`) via the `apply` overloads in the companion.
  *
  * @param topic
  *   the topic the record belongs to
  * @param partition
  *   the partition index
  * @param offset
  *   the record's offset within the partition
  * @param timestamp
  *   the record timestamp in epoch milliseconds
  * @param timestampType
  *   the Kafka timestamp-type id (0 = CreateTime, 1 = LogAppendTime), or `None` when not available (e.g. a
  *   producer's `RecordMetadata`, or an absent Avro field)
  * @param serializedKeySize
  *   size in bytes of the serialized key (-1 if the key was null)
  * @param serializedValueSize
  *   size in bytes of the serialized value (-1 if the value was null)
  */
final case class MetaInfo(
  topic: String,
  partition: Int,
  offset: Long,
  timestamp: Long,
  timestampType: Option[Int],
  serializedKeySize: Int,
  serializedValueSize: Int
) derives Codec.AsObject {

  /** The record timestamp rendered as a `LocalDateTime` in the given zone. */
  def localDateTime(zoneId: ZoneId): LocalDateTime =
    Instant.ofEpochMilli(timestamp).atZone(zoneId).toLocalDateTime

  /** A view with the timestamp resolved to a `ZonedDateTime` in the given zone (see `ZonedMetaInfo`). */
  def zoned(zoneId: ZoneId): ZonedMetaInfo =
    this
      .into[ZonedMetaInfo]
      .withFieldComputed(_.timestamp, cr => Instant.ofEpochMilli(cr.timestamp).atZone(zoneId))
      .transform
}

object MetaInfo {

  /** Extract metadata from an internal `NJConsumerRecord`. */
  def apply[K, V](cr: NJConsumerRecord[K, V]): MetaInfo =
    cr.into[MetaInfo].transform

  /** Extract metadata from an fs2-kafka `ConsumerRecord`. */
  def apply[K, V](fcr: Fs2ConsumerRecord[K, V]): MetaInfo =
    apply(fcr.into[NJConsumerRecord[K, V]].transform)

  /** Extract metadata from an fs2-kafka `CommittableConsumerRecord` (ignoring the commit offset). */
  def apply[F[_], K, V](ccr: CommittableConsumerRecord[F, K, V]): MetaInfo =
    apply(ccr.record.into[NJConsumerRecord[K, V]].transform)

  /** Extract metadata from a Java Kafka-client `ConsumerRecord`. */
  def apply[K, V](jcr: JavaConsumerRecord[K, V]): MetaInfo =
    apply(jcr.into[NJConsumerRecord[K, V]].transform)

  // GenericRecord.get returns Object; these read a field with the expected numeric type and, on a mismatch,
  // yield a Failure naming the field and the type actually found, instead of an opaque ClassCastException.
  private def typeName(o: Any): String = Option(o).fold("null")(_.getClass.getName)

  private def getInt(gr: GenericRecord, field: String): Try[Int] =
    gr.get(field) match {
      case i: java.lang.Integer => Success(i.intValue)
      case other => Failure(new IllegalArgumentException(s"$field: expected int, got ${typeName(other)}"))
    }

  private def getLong(gr: GenericRecord, field: String): Try[Long] =
    gr.get(field) match {
      case l: java.lang.Long    => Success(l.longValue)
      case i: java.lang.Integer => Success(i.longValue) // tolerate a writer that used int for a long field
      case other => Failure(new IllegalArgumentException(s"$field: expected long, got ${typeName(other)}"))
    }

  // None when the field is null/absent; otherwise the int value (Failure on a type mismatch).
  private def getIntOpt(gr: GenericRecord, field: String): Try[Option[Int]] =
    Option(gr.get(field)).traverse {
      case i: java.lang.Integer => Success(i.intValue)
      case other => Failure(new IllegalArgumentException(s"$field: expected int, got ${typeName(other)}"))
    }

  /** Extract metadata from an Avro `GenericRecord` by reading its metadata fields.
    *
    * Fallible because a `GenericRecord` is untyped: a missing or wrong-typed field yields a `Failure` whose
    * message names the offending field and the type found. `timestampType` is optional (`None` when the field
    * is null/absent); long fields also accept an int value.
    */
  def apply(gr: GenericRecord): Try[MetaInfo] =
    for {
      partition <- getInt(gr, "partition")
      offset <- getLong(gr, "offset")
      timestamp <- getLong(gr, "timestamp")
      timestampType <- getIntOpt(gr, "timestampType")
      serializedKeySize <- getInt(gr, "serializedKeySize")
      serializedValueSize <- getInt(gr, "serializedValueSize")
    } yield MetaInfo(
      topic = gr.get("topic").toString,
      partition = partition,
      offset = offset,
      timestamp = timestamp,
      timestampType = timestampType,
      serializedKeySize = serializedKeySize,
      serializedValueSize = serializedValueSize
    )

  /** Extract metadata from a producer's `RecordMetadata`. `timestampType` is `None` since the producer ack
    * does not carry it.
    */
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

/** A reporting-friendly view of `MetaInfo` with the timestamp resolved to a `ZonedDateTime` in a chosen zone.
  * Produced by `MetaInfo.zoned`.
  */
final case class ZonedMetaInfo(topic: String, partition: Int, offset: Long, timestamp: ZonedDateTime)
    derives Codec.AsObject
