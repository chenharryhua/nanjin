package com.github.chenharryhua.nanjin.messages.kafka

import fs2.kafka.*
import io.scalaland.chimney.Transformer
import io.scalaland.chimney.dsl.*
import monocle.Iso
import org.apache.kafka.clients.consumer.ConsumerRecord as KafkaConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord as KafkaProducerRecord
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.record.TimestampType

import scala.jdk.OptionConverters.{RichOption, RichOptional}

private[kafka] trait Isos {
  implicit val isoNJHeader: Iso[NJHeader, Header] =
    Iso[NJHeader, Header](nj => Header(nj.key, nj.value))(r => NJHeader(r.key(), r.value()))

  implicit def isoIdentityProducerRecord[K, V]: Iso[KafkaProducerRecord[K, V], KafkaProducerRecord[K, V]] =
    Iso[KafkaProducerRecord[K, V], KafkaProducerRecord[K, V]](identity)(identity)

  implicit def isoIdentityConsumerRecord[K, V]: Iso[KafkaConsumerRecord[K, V], KafkaConsumerRecord[K, V]] =
    Iso[KafkaConsumerRecord[K, V], KafkaConsumerRecord[K, V]](identity)(identity)

  implicit def isoFs2ProducerRecord[K, V]: Iso[ProducerRecord[K, V], KafkaProducerRecord[K, V]] =
    Iso[ProducerRecord[K, V], KafkaProducerRecord[K, V]](_.transformInto)(_.transformInto)

  implicit def isoFs2ComsumerRecord[K, V]: Iso[ConsumerRecord[K, V], KafkaConsumerRecord[K, V]] =
    Iso[ConsumerRecord[K, V], KafkaConsumerRecord[K, V]](_.transformInto)(_.transformInto)

  implicit def fromJavaProducerRecordTransformer[K, V]
    : Transformer[KafkaProducerRecord[K, V], ProducerRecord[K, V]] =
    (pr: KafkaProducerRecord[K, V]) =>
      ProducerRecord(pr.topic, pr.key, pr.value)
        .withPartition(pr.partition)
        .withTimestamp(pr.timestamp)
        .withHeaders(pr.headers.toArray.foldLeft(Headers.empty)((t, i) => t.append(i.key, i.value)))

  implicit def toJavaProducerRecordTransformer[K, V]
    : Transformer[ProducerRecord[K, V], KafkaProducerRecord[K, V]] =
    (fpr: ProducerRecord[K, V]) =>
      new KafkaProducerRecord[K, V](
        fpr.topic,
        fpr.partition.map(Integer.valueOf).orNull,
        fpr.timestamp.map(java.lang.Long.valueOf).orNull,
        fpr.key,
        fpr.value,
        fpr.headers.asJava)

  implicit def fromJavaConsumerRecordTransformer[K, V]
    : Transformer[KafkaConsumerRecord[K, V], ConsumerRecord[K, V]] =
    (cr: KafkaConsumerRecord[K, V]) => {
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
    : Transformer[ConsumerRecord[K, V], KafkaConsumerRecord[K, V]] =
    (fcr: ConsumerRecord[K, V]) =>
      new KafkaConsumerRecord[K, V](
        fcr.topic,
        fcr.partition,
        fcr.offset,
        fcr.timestamp.createTime
          .orElse(fcr.timestamp.logAppendTime)
          .orElse(fcr.timestamp.unknownTime)
          .getOrElse(KafkaConsumerRecord.NO_TIMESTAMP),
        fcr.timestamp.createTime
          .map(_ => TimestampType.CREATE_TIME)
          .orElse(fcr.timestamp.logAppendTime.map(_ => TimestampType.LOG_APPEND_TIME))
          .getOrElse(TimestampType.NO_TIMESTAMP_TYPE),
        fcr.serializedKeySize.getOrElse(KafkaConsumerRecord.NULL_SIZE),
        fcr.serializedValueSize.getOrElse(KafkaConsumerRecord.NULL_SIZE),
        fcr.key,
        fcr.value,
        new RecordHeaders(fcr.headers.asJava),
        fcr.leaderEpoch.map(Integer.valueOf).toJava
      )
}
