package com.github.chenharryhua.nanjin.messages.kafka

import fs2.kafka.{
  ConsumerRecord as Fs2ConsumerRecord,
  Headers as Fs2Headers,
  ProducerRecord as Fs2ProducerRecord,
  Timestamp as Fs2Timestamp
}
import io.scalaland.chimney.Transformer
import io.scalaland.chimney.dsl.*
import monocle.Iso
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.record.TimestampType

import scala.compat.java8.OptionConverters.*

private[kafka] trait Isos {

  implicit def isoIdentityProducerRecord[K, V]: Iso[ProducerRecord[K, V], ProducerRecord[K, V]] =
    Iso[ProducerRecord[K, V], ProducerRecord[K, V]](identity)(identity)

  implicit def isoIdentityConsumerRecord[K, V]: Iso[ConsumerRecord[K, V], ConsumerRecord[K, V]] =
    Iso[ConsumerRecord[K, V], ConsumerRecord[K, V]](identity)(identity)

  implicit def isoFs2ProducerRecord[K, V]: Iso[Fs2ProducerRecord[K, V], ProducerRecord[K, V]] =
    Iso[Fs2ProducerRecord[K, V], ProducerRecord[K, V]](_.transformInto)(_.transformInto)

  implicit def isoFs2ComsumerRecord[K, V]: Iso[Fs2ConsumerRecord[K, V], ConsumerRecord[K, V]] =
    Iso[Fs2ConsumerRecord[K, V], ConsumerRecord[K, V]](_.transformInto)(_.transformInto)

  implicit def fromProducerRecord[K, V]: Transformer[ProducerRecord[K, V], Fs2ProducerRecord[K, V]] =
    (pr: ProducerRecord[K, V]) =>
      Fs2ProducerRecord(pr.topic, pr.key, pr.value)
        .withPartition(pr.partition)
        .withTimestamp(pr.timestamp)
        .withHeaders(pr.headers.toArray.foldLeft(Fs2Headers.empty)((t, i) => t.append(i.key, i.value)))

  implicit def toProducerRecord[K, V]: Transformer[Fs2ProducerRecord[K, V], ProducerRecord[K, V]] =
    (fpr: Fs2ProducerRecord[K, V]) =>
      new ProducerRecord[K, V](
        fpr.topic,
        fpr.partition.map(Integer.valueOf).orNull,
        fpr.timestamp.map(java.lang.Long.valueOf).orNull,
        fpr.key,
        fpr.value,
        fpr.headers.asJava)

  implicit def fromConsumerRecord[K, V]: Transformer[ConsumerRecord[K, V], Fs2ConsumerRecord[K, V]] =
    (cr: ConsumerRecord[K, V]) => {
      val epoch: Option[Int] = cr.leaderEpoch().asScala.map(_.intValue())
      val fcr =
        Fs2ConsumerRecord[K, V](cr.topic(), cr.partition(), cr.offset(), cr.key(), cr.value())
          .withHeaders(cr.headers.toArray.foldLeft(Fs2Headers.empty)((t, i) => t.append(i.key, i.value)))
          .withSerializedKeySize(cr.serializedKeySize())
          .withSerializedValueSize(cr.serializedValueSize())
          .withTimestamp(cr.timestampType match {
            case TimestampType.CREATE_TIME       => Fs2Timestamp.createTime(cr.timestamp())
            case TimestampType.LOG_APPEND_TIME   => Fs2Timestamp.logAppendTime(cr.timestamp())
            case TimestampType.NO_TIMESTAMP_TYPE => Fs2Timestamp.unknownTime(cr.timestamp())
          })
      epoch.fold[Fs2ConsumerRecord[K, V]](fcr)(e => fcr.withLeaderEpoch(e))
    }

  implicit def toConsumerRecord[K, V]: Transformer[Fs2ConsumerRecord[K, V], ConsumerRecord[K, V]] =
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
        fcr.serializedKeySize.getOrElse(ConsumerRecord.NULL_SIZE),
        fcr.serializedValueSize.getOrElse(ConsumerRecord.NULL_SIZE),
        fcr.key,
        fcr.value,
        new RecordHeaders(fcr.headers.asJava),
        fcr.leaderEpoch.map(Integer.valueOf).asJava
      )
}
