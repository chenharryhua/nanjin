package com.github.chenharryhua.nanjin.messages.kafka

import fs2.kafka.*
import io.scalaland.chimney.Transformer
import io.scalaland.chimney.dsl.*
import monocle.Iso
import org.apache.kafka.clients.consumer.ConsumerRecord as JavaConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord as JavaProducerRecord
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.record.TimestampType

import scala.jdk.OptionConverters.{RichOption, RichOptional}

private[kafka] trait Isos {
  implicit val isoNJHeader: Iso[Header, Header] =
    Iso[Header, Header](nj => Header(nj.key, nj.value))(r => Header(r.key(), r.value()))

  implicit def isoIdentityProducerRecord[K, V]: Iso[JavaProducerRecord[K, V], JavaProducerRecord[K, V]] =
    Iso[JavaProducerRecord[K, V], JavaProducerRecord[K, V]](identity)(identity)

  implicit def isoIdentityConsumerRecord[K, V]: Iso[JavaConsumerRecord[K, V], JavaConsumerRecord[K, V]] =
    Iso[JavaConsumerRecord[K, V], JavaConsumerRecord[K, V]](identity)(identity)

  implicit def isoFs2ProducerRecord[K, V]: Iso[ProducerRecord[K, V], JavaProducerRecord[K, V]] =
    Iso[ProducerRecord[K, V], JavaProducerRecord[K, V]](_.transformInto)(_.transformInto)

  implicit def isoFs2ConsumerRecord[K, V]: Iso[ConsumerRecord[K, V], JavaConsumerRecord[K, V]] =
    Iso[ConsumerRecord[K, V], JavaConsumerRecord[K, V]](_.transformInto)(_.transformInto)

  implicit def fromJavaProducerRecordTransformer[K, V]
    : Transformer[JavaProducerRecord[K, V], ProducerRecord[K, V]] =
    (pr: JavaProducerRecord[K, V]) =>
      ProducerRecord(pr.topic, pr.key, pr.value)
        .withPartition(pr.partition)
        .withTimestamp(pr.timestamp)
        .withHeaders(pr.headers.toArray.foldLeft(Headers.empty)((t, i) => t.append(i.key, i.value)))

  implicit def toJavaProducerRecordTransformer[K, V]
    : Transformer[ProducerRecord[K, V], JavaProducerRecord[K, V]] =
    (fpr: ProducerRecord[K, V]) =>
      new JavaProducerRecord[K, V](
        fpr.topic,
        fpr.partition.map(Integer.valueOf).orNull,
        fpr.timestamp.map(java.lang.Long.valueOf).orNull,
        fpr.key,
        fpr.value,
        fpr.headers.asJava)

  implicit def fromJavaConsumerRecordTransformer[K, V]
    : Transformer[JavaConsumerRecord[K, V], ConsumerRecord[K, V]] =
    (cr: JavaConsumerRecord[K, V]) => {
      val epoch: Option[Int] = cr.leaderEpoch().toScala.map(_.intValue())
      val fcr =
        ConsumerRecord[K, V](cr.topic(), cr.partition(), cr.offset(), cr.key(), cr.value())
          .withHeaders(cr.headers.toArray.foldLeft(Headers.empty)((t, i) => t.append(i.key, i.value)))
          .withSerializedKeySize(cr.serializedKeySize())
          .withSerializedValueSize(cr.serializedValueSize())
          .withTimestamp(cr.timestampType match {
            case TimestampType.CREATE_TIME       => Timestamp.createTime(cr.timestamp())
            case TimestampType.LOG_APPEND_TIME   => Timestamp.logAppendTime(cr.timestamp())
            case TimestampType.NO_TIMESTAMP_TYPE => Timestamp.unknownTime(cr.timestamp())
          })
      epoch.fold[ConsumerRecord[K, V]](fcr)(e => fcr.withLeaderEpoch(e))
    }

  implicit def toJavaConsumerRecordTransformer[K, V]
    : Transformer[ConsumerRecord[K, V], JavaConsumerRecord[K, V]] =
    (fcr: ConsumerRecord[K, V]) =>
      new JavaConsumerRecord[K, V](
        fcr.topic,
        fcr.partition,
        fcr.offset,
        fcr.timestamp.createTime
          .orElse(fcr.timestamp.logAppendTime)
          .orElse(fcr.timestamp.unknownTime)
          .getOrElse(JavaConsumerRecord.NO_TIMESTAMP),
        fcr.timestamp.createTime
          .map(_ => TimestampType.CREATE_TIME)
          .orElse(fcr.timestamp.logAppendTime.map(_ => TimestampType.LOG_APPEND_TIME))
          .getOrElse(TimestampType.NO_TIMESTAMP_TYPE),
        fcr.serializedKeySize.getOrElse(JavaConsumerRecord.NULL_SIZE),
        fcr.serializedValueSize.getOrElse(JavaConsumerRecord.NULL_SIZE),
        fcr.key,
        fcr.value,
        new RecordHeaders(fcr.headers.asJava),
        fcr.leaderEpoch.map(Integer.valueOf).toJava
      )
}
