package com.github.chenharryhua.nanjin.kafka.codec

import fs2.kafka.{
  ConsumerRecord => Fs2ConsumerRecord,
  Headers => Fs2Headers,
  ProducerRecord => Fs2ProducerRecord,
  Timestamp => Fs2Timestamp
}
import io.scalaland.chimney.Transformer
import io.scalaland.chimney.dsl._
import monocle.Iso
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.record.TimestampType

import scala.compat.java8.OptionConverters._

object iso {

  implicit def isoIdentityProducerRecord[K, V]: Iso[ProducerRecord[K, V], ProducerRecord[K, V]] =
    Iso[ProducerRecord[K, V], ProducerRecord[K, V]](identity)(identity)

  implicit def isoIdentityConsumerRecord[K, V]: Iso[ConsumerRecord[K, V], ConsumerRecord[K, V]] =
    Iso[ConsumerRecord[K, V], ConsumerRecord[K, V]](identity)(identity)

  implicit def isoFs2ProducerRecord[K, V]: Iso[Fs2ProducerRecord[K, V], ProducerRecord[K, V]] =
    Iso[Fs2ProducerRecord[K, V], ProducerRecord[K, V]](_.transformInto)(_.transformInto)

  implicit def isoFs2ComsumerRecord[K, V]: Iso[Fs2ConsumerRecord[K, V], ConsumerRecord[K, V]] =
    Iso[Fs2ConsumerRecord[K, V], ConsumerRecord[K, V]](_.transformInto)(_.transformInto)

  implicit def toFs2ProducerRecord[K, V]
    : Transformer[ProducerRecord[K, V], Fs2ProducerRecord[K, V]] =
    (pr: ProducerRecord[K, V]) =>
      Fs2ProducerRecord(pr.topic, pr.key, pr.value)
        .withPartition(pr.partition)
        .withTimestamp(pr.timestamp)
        .withHeaders(pr.headers.toArray.foldLeft(Fs2Headers.empty)((t, i) =>
          t.append(i.key, i.value)))

  implicit def fromFs2ProducerRecord[K, V]
    : Transformer[Fs2ProducerRecord[K, V], ProducerRecord[K, V]] =
    (fpr: Fs2ProducerRecord[K, V]) =>
      new ProducerRecord[K, V](
        fpr.topic,
        fpr.partition.map(new java.lang.Integer(_)).orNull,
        fpr.timestamp.map(new java.lang.Long(_)).orNull,
        fpr.key,
        fpr.value,
        fpr.headers.asJava)

  implicit def fromFs2ConsumerRecord[K, V]
    : Transformer[ConsumerRecord[K, V], Fs2ConsumerRecord[K, V]] =
    (cr: ConsumerRecord[K, V]) => {
      val epoch: Option[Int] = cr.leaderEpoch().asScala.map(_.intValue())
      val fcr =
        Fs2ConsumerRecord[K, V](cr.topic(), cr.partition(), cr.offset(), cr.key(), cr.value())
          .withHeaders(cr.headers.toArray.foldLeft(Fs2Headers.empty)((t, i) =>
            t.append(i.key, i.value)))
          .withSerializedKeySize(cr.serializedKeySize())
          .withSerializedValueSize(cr.serializedValueSize())
          .withTimestamp(cr.timestampType match {
            case TimestampType.CREATE_TIME       => Fs2Timestamp.createTime(cr.timestamp())
            case TimestampType.LOG_APPEND_TIME   => Fs2Timestamp.logAppendTime(cr.timestamp())
            case TimestampType.NO_TIMESTAMP_TYPE => Fs2Timestamp.unknownTime(cr.timestamp())
          })
      epoch.fold[Fs2ConsumerRecord[K, V]](fcr)(e => fcr.withLeaderEpoch(e))
    }

  implicit def toFs2ConsumerRecord[K, V]
    : Transformer[Fs2ConsumerRecord[K, V], ConsumerRecord[K, V]] =
    (fcr: Fs2ConsumerRecord[K, V]) =>
      new ConsumerRecord[K, V](
        fcr.topic,
        fcr.partition,
        fcr.offset,
        fcr.timestamp.createTime
          .orElse(fcr.timestamp.logAppendTime)
          .orElse(fcr.timestamp.unknownTime)
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
}
