package com.github.chenharryhua.nanjin.codec
import cats.Eq
import cats.implicits._
import fs2.kafka.{
  CommittableConsumerRecord,
  CommittableOffset,
  Headers,
  ProducerRecords,
  Timestamp,
  ConsumerRecord => Fs2ConsumerRecord,
  ProducerRecord => Fs2ProducerRecord
}
import monocle.Iso
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.record.TimestampType

import scala.compat.java8.OptionConverters._

trait MessagePropertiesFs2 extends BitraverseKafkaRecord {

  final def fromKafkaProducerRecord[K, V](pr: ProducerRecord[K, V]): Fs2ProducerRecord[K, V] =
    Fs2ProducerRecord(pr.topic, pr.key, pr.value)
      .withPartition(pr.partition)
      .withTimestamp(pr.timestamp)
      .withHeaders(pr.headers.toArray.foldLeft(Headers.empty)((t, i) => t.append(i.key, i.value)))

  final def toKafkaProducerRecord[K, V](fpr: Fs2ProducerRecord[K, V]): ProducerRecord[K, V] =
    new ProducerRecord[K, V](
      fpr.topic,
      fpr.partition.map(new java.lang.Integer(_)).orNull,
      fpr.timestamp.map(new java.lang.Long(_)).orNull,
      fpr.key,
      fpr.value,
      fpr.headers.asJava)

  final def isoFs2ProducerRecord[K, V]: Iso[Fs2ProducerRecord[K, V], ProducerRecord[K, V]] =
    Iso[Fs2ProducerRecord[K, V], ProducerRecord[K, V]](toKafkaProducerRecord)(
      fromKafkaProducerRecord)

  final def fromKafkaConsumerRecord[K, V](cr: ConsumerRecord[K, V]): Fs2ConsumerRecord[K, V] = {
    val epoch: Option[Int] = cr.leaderEpoch().asScala.map(_.intValue())
    val fcr =
      Fs2ConsumerRecord[K, V](cr.topic(), cr.partition(), cr.offset(), cr.key(), cr.value())
        .withHeaders(cr.headers.toArray.foldLeft(Headers.empty)((t, i) => t.append(i.key, i.value)))
        .withSerializedKeySize(cr.serializedKeySize())
        .withSerializedValueSize(cr.serializedValueSize())
        .withTimestamp(cr.timestampType match {
          case TimestampType.CREATE_TIME       => Timestamp.createTime(cr.timestamp())
          case TimestampType.LOG_APPEND_TIME   => Timestamp.logAppendTime(cr.timestamp())
          case TimestampType.NO_TIMESTAMP_TYPE => Timestamp.none
        })
    epoch.fold[Fs2ConsumerRecord[K, V]](fcr)(e => fcr.withLeaderEpoch(e))
  }

  final def toKafkaConsumerRecord[K, V](fcr: Fs2ConsumerRecord[K, V]): ConsumerRecord[K, V] =
    new ConsumerRecord[K, V](
      fcr.topic,
      fcr.partition,
      fcr.offset,
      fcr.timestamp.createTime
        .orElse(fcr.timestamp.logAppendTime)
        .getOrElse(ConsumerRecord.NO_TIMESTAMP),
      fcr.timestamp.createTime
        .map(_ => TimestampType.CREATE_TIME)
        .orElse(fcr.timestamp.logAppendTime.map(_ => TimestampType.LOG_APPEND_TIME))
        .getOrElse(TimestampType.NO_TIMESTAMP_TYPE),
      -1L, //ConsumerRecord.NULL_CHECKSUM,
      fcr.serializedKeySize.getOrElse(ConsumerRecord.NULL_SIZE),
      fcr.serializedValueSize.getOrElse(ConsumerRecord.NULL_SIZE),
      fcr.key,
      fcr.value,
      new RecordHeaders(fcr.headers.asJava),
      fcr.leaderEpoch.map(new Integer(_)).asJava
    )

  final def isoFs2ComsumerRecord[K, V]: Iso[Fs2ConsumerRecord[K, V], ConsumerRecord[K, V]] =
    Iso[Fs2ConsumerRecord[K, V], ConsumerRecord[K, V]](toKafkaConsumerRecord)(
      fromKafkaConsumerRecord)

  implicit def eqCommittableOffsetFs2[F[_]]: Eq[CommittableOffset[F]] =
    (x: CommittableOffset[F], y: CommittableOffset[F]) =>
      (x.topicPartition === y.topicPartition) &&
        (x.consumerGroupId === y.consumerGroupId) &&
        (x.offsetAndMetadata === y.offsetAndMetadata) &&
        (x.offsets === y.offsets)

  implicit final def eqConsumerRecordFs2[K: Eq, V: Eq]: Eq[Fs2ConsumerRecord[K, V]] =
    (x: Fs2ConsumerRecord[K, V], y: Fs2ConsumerRecord[K, V]) =>
      isoFs2ComsumerRecord.get(x) === isoFs2ComsumerRecord.get(y)

  implicit final def eqProducerRecordFs2[K: Eq, V: Eq]: Eq[Fs2ProducerRecord[K, V]] =
    (x: Fs2ProducerRecord[K, V], y: Fs2ProducerRecord[K, V]) =>
      isoFs2ProducerRecord.get(x) === isoFs2ProducerRecord.get(y)

  implicit final def eqProducerRecordsFs2[K: Eq, V: Eq, P: Eq]: Eq[ProducerRecords[K, V, P]] =
    (x: ProducerRecords[K, V, P], y: ProducerRecords[K, V, P]) =>
      (x.records === y.records) &&
        (x.passthrough === y.passthrough)

  implicit final def eqCommittableConsumerRecordFs2[F[_], K: Eq, V: Eq]
    : Eq[CommittableConsumerRecord[F, K, V]] =
    (x: CommittableConsumerRecord[F, K, V], y: CommittableConsumerRecord[F, K, V]) =>
      (x.record === y.record) && (x.offset === y.offset)
}
