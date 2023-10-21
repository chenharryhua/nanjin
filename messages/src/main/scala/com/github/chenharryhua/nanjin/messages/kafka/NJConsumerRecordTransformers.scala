package com.github.chenharryhua.nanjin.messages.kafka

import cats.data.Cont
import fs2.kafka.*
import io.scalaland.chimney.Transformer
import io.scalaland.chimney.dsl.*
import org.apache.kafka.clients.consumer.ConsumerRecord as JavaConsumerRecord
import org.apache.kafka.common.header.Header as JavaHeader
import org.apache.kafka.common.header.internals.{RecordHeader, RecordHeaders}
import org.apache.kafka.common.record.TimestampType as JavaTimestampType

import scala.jdk.OptionConverters.{RichOption, RichOptional}

private trait NJHeaderTransformers {

  implicit val transformerHeaderNJFs2: Transformer[NJHeader, Header] =
    (src: NJHeader) => Header(src.key, src.value)
  implicit val transformHeaderFs2NJ: Transformer[Header, NJHeader] =
    (src: Header) => NJHeader(src.key(), src.value())

  implicit val transformHeaderJavaNJ: Transformer[JavaHeader, NJHeader] =
    (src: JavaHeader) => NJHeader(src.key(), src.value())

  implicit val transformHeaderNJJava: Transformer[NJHeader, JavaHeader] =
    (src: NJHeader) => new RecordHeader(src.key, src.value)
}

private[kafka] trait NJConsumerRecordTransformers extends NJHeaderTransformers {

  implicit def transformCRJavaNJ[K, V]: Transformer[JavaConsumerRecord[K, V], NJConsumerRecord[K, V]] =
    (src: JavaConsumerRecord[K, V]) =>
      NJConsumerRecord(
        topic = src.topic(),
        partition = src.partition(),
        offset = src.offset(),
        timestamp = src.timestamp(),
        timestampType = src.timestampType().id,
        serializedKeySize = Option(src.serializedKeySize()),
        serializedValueSize = Option(src.serializedValueSize()),
        key = Option(src.key()),
        value = Option(src.value()),
        headers = src.headers().toArray.map(_.transformInto[NJHeader]).toList,
        leaderEpoch = src.leaderEpoch().toScala.map(_.toInt)
      )

  implicit def transformCRNJJava[K, V]: Transformer[NJConsumerRecord[K, V], JavaConsumerRecord[K, V]] =
    (src: NJConsumerRecord[K, V]) =>
      new JavaConsumerRecord[K, V](
        src.topic,
        src.partition,
        src.offset,
        src.timestamp,
        src.timestampType match {
          case 0 => JavaTimestampType.CREATE_TIME
          case 1 => JavaTimestampType.LOG_APPEND_TIME
          case _ => JavaTimestampType.NO_TIMESTAMP_TYPE
        },
        src.serializedKeySize.getOrElse(JavaConsumerRecord.NULL_SIZE),
        src.serializedValueSize.getOrElse(JavaConsumerRecord.NULL_SIZE),
        src.key.getOrElse(null.asInstanceOf[K]),
        src.value.getOrElse(null.asInstanceOf[V]),
        new RecordHeaders(src.headers.map(_.transformInto[JavaHeader]).toArray),
        src.leaderEpoch.map(Integer.valueOf).toJava
      )

  implicit def transformCRFs2NJ[K, V]: Transformer[ConsumerRecord[K, V], NJConsumerRecord[K, V]] =
    (src: ConsumerRecord[K, V]) => {
      val (timestampType, timestamp) =
        src.timestamp.createTime
          .map((JavaTimestampType.CREATE_TIME.id, _))
          .orElse(src.timestamp.logAppendTime.map((JavaTimestampType.LOG_APPEND_TIME.id, _)))
          .orElse(src.timestamp.unknownTime.map((JavaTimestampType.NO_TIMESTAMP_TYPE.id, _)))
          .getOrElse((JavaTimestampType.NO_TIMESTAMP_TYPE.id, JavaConsumerRecord.NO_TIMESTAMP))

      NJConsumerRecord(
        topic = src.topic,
        partition = src.partition,
        offset = src.offset,
        timestamp = timestamp,
        timestampType = timestampType,
        serializedKeySize = src.serializedKeySize,
        serializedValueSize = src.serializedValueSize,
        key = Option(src.key),
        value = Option(src.value),
        headers = src.headers.toChain.map(_.transformInto[NJHeader]).toList,
        leaderEpoch = src.leaderEpoch
      )
    }

  implicit def transformCRNJFs2[K, V]: Transformer[NJConsumerRecord[K, V], ConsumerRecord[K, V]] =
    (src: NJConsumerRecord[K, V]) =>
      Cont
        .pure(
          ConsumerRecord[K, V](
            topic = src.topic,
            partition = src.partition,
            offset = src.offset,
            key = src.key.getOrElse(null.asInstanceOf[K]),
            value = src.value.getOrElse(null.asInstanceOf[V]))
            .withTimestamp(src.timestampType match {
              case JavaTimestampType.CREATE_TIME.id =>
                Timestamp.createTime(src.timestamp)
              case JavaTimestampType.LOG_APPEND_TIME.id =>
                Timestamp.logAppendTime(src.timestamp)
              case _ =>
                Timestamp.unknownTime(src.timestamp)
            })
            .withHeaders(Headers.fromSeq(src.headers.map(_.transformInto[Header]))))
        .map(cr => src.serializedKeySize.fold(cr)(cr.withSerializedKeySize))
        .map(cr => src.serializedValueSize.fold(cr)(cr.withSerializedValueSize))
        .map(cr => src.leaderEpoch.fold(cr)(cr.withLeaderEpoch))
        .eval
        .value
}
