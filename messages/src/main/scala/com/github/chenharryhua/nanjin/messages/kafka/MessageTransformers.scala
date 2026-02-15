package com.github.chenharryhua.nanjin.messages.kafka
import cats.data.Cont
import cats.syntax.eq.catsSyntaxEq
import fs2.kafka.*
import io.scalaland.chimney.Transformer
import io.scalaland.chimney.dsl.*
import org.apache.kafka.clients.consumer.ConsumerRecord as JavaConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord as JavaProducerRecord
import org.apache.kafka.common.header.{Header as JavaHeader, Headers as JavaHeaders}
import org.apache.kafka.common.record.TimestampType as JavaTimestampType

import scala.jdk.OptionConverters.{RichOption, RichOptional}

private trait MessageTransformers {

  implicit val transformHeaderJavaFs2: Transformer[JavaHeader, Header] =
    (src: JavaHeader) => Header(src.key(), src.value())

  implicit val transformHeadersJavaFs2: Transformer[JavaHeaders, Headers] =
    (src: JavaHeaders) => Headers.fromSeq(src.toArray.transformInto[Seq[Header]])

  implicit val transformHeaderFs2Java: Transformer[Header, JavaHeader] =
    (src: Header) => src

  implicit val transformHeadersFs2Java: Transformer[Headers, JavaHeaders] =
    (src: Headers) => src.asJava

  implicit def transformCRJavaFs2[K, V]: Transformer[JavaConsumerRecord[K, V], ConsumerRecord[K, V]] =
    (src: JavaConsumerRecord[K, V]) =>
      Cont
        .pure(
          ConsumerRecord(
            topic = src.topic(),
            partition = src.partition(),
            offset = src.offset(),
            key = src.key(),
            value = src.value()
          ).withHeaders(src.headers().transformInto[Headers]))
        .map(cr => src.leaderEpoch().toScala.fold(cr)(cr.withLeaderEpoch(_)))
        .map(cr =>
          src.timestampType() match {
            case JavaTimestampType.NO_TIMESTAMP_TYPE =>
              cr.withTimestamp(Timestamp.unknownTime(src.timestamp()))
            case JavaTimestampType.CREATE_TIME =>
              cr.withTimestamp(Timestamp.createTime(src.timestamp()))
            case JavaTimestampType.LOG_APPEND_TIME =>
              cr.withTimestamp(Timestamp.logAppendTime(src.timestamp()))
          })
        .map(cr =>
          if (src.serializedKeySize() === JavaConsumerRecord.NULL_SIZE) cr
          else cr.withSerializedKeySize(src.serializedKeySize()))
        .map(cr =>
          if (src.serializedValueSize() === JavaConsumerRecord.NULL_SIZE) cr
          else cr.withSerializedValueSize(src.serializedValueSize()))
        .eval
        .value

  implicit def transformCRFs2Java[K, V]: Transformer[ConsumerRecord[K, V], JavaConsumerRecord[K, V]] =
    (src: ConsumerRecord[K, V]) => {
      val (timestampType, timestamp) =
        src.timestamp.createTime
          .map((JavaTimestampType.CREATE_TIME, _))
          .orElse(src.timestamp.logAppendTime.map((JavaTimestampType.LOG_APPEND_TIME, _)))
          .orElse(src.timestamp.unknownTime.map((JavaTimestampType.NO_TIMESTAMP_TYPE, _)))
          .getOrElse((JavaTimestampType.NO_TIMESTAMP_TYPE, JavaConsumerRecord.NO_TIMESTAMP))

      new JavaConsumerRecord[K, V](
        src.topic,
        src.partition,
        src.offset,
        timestamp,
        timestampType,
        src.serializedKeySize.getOrElse(JavaConsumerRecord.NULL_SIZE),
        src.serializedValueSize.getOrElse(JavaConsumerRecord.NULL_SIZE),
        src.key,
        src.value,
        src.headers.transformInto[JavaHeaders],
        src.leaderEpoch.map(Integer.valueOf).toJava
      )
    }

  implicit def transformPRFs2Java[K, V]: Transformer[ProducerRecord[K, V], JavaProducerRecord[K, V]] =
    (src: ProducerRecord[K, V]) =>
      new JavaProducerRecord[K, V](
        src.topic,
        src.partition.map(Integer.valueOf).orNull,
        src.timestamp.map(java.lang.Long.valueOf).orNull,
        src.key,
        src.value,
        src.headers.transformInto[JavaHeaders]
      )

  implicit def transformPRJavaFs2[K, V]
    : Transformer[JavaProducerRecord[K, V], ProducerRecord[K, V]] = { (src: JavaProducerRecord[K, V]) =>
    Cont
      .pure(
        ProducerRecord(src.topic(), src.key(), src.value()).withHeaders(src.headers().transformInto[Headers]))
      .map(pr => Option(src.partition()).fold(pr)(pr.withPartition(_)))
      .map(pr => Option(src.timestamp()).fold(pr)(pr.withTimestamp(_)))
      .eval
      .value
  }
}
