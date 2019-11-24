package com.github.chenharryhua.nanjin.kafka.internal

import java.time.Duration

import cats.effect.Sync
import cats.implicits._
import com.github.chenharryhua.nanjin.datetime.{NJDateTimeRange, NJTimestamp}
import com.github.chenharryhua.nanjin.kafka.{GenericTopicPartition, KafkaOffset, KafkaOffsetRange}
import fs2.kafka.{KafkaByteConsumer, KafkaByteConsumerRecord}
import org.apache.kafka.clients.consumer.OffsetAndTimestamp
import org.apache.kafka.common.{PartitionInfo, TopicPartition}

import scala.collection.JavaConverters._

final class ConsumerWrapper[F[_]](consumer: KafkaByteConsumer)(implicit F: Sync[F]) {

  def partitionsFor(topicName: String): F[GenericTopicPartition[PartitionInfo]] =
    F.delay(
        consumer
          .partitionsFor(topicName)
          .asScala
          .toList
          .mapFilter(Option(_))
          .map(info => new TopicPartition(info.topic, info.partition) -> info)
          .toMap)
      .map(GenericTopicPartition(_))

  def beginningOffsets(topicName: String): F[GenericTopicPartition[Option[KafkaOffset]]] =
    for {
      tps <- partitionsFor(topicName).map(_.topicPartitions)
      ret <- F.delay(
        consumer
          .beginningOffsets(tps.asJava)
          .asScala
          .toMap
          .mapValues(Option(_).map(KafkaOffset(_))))
    } yield GenericTopicPartition(ret)

  def endOffsets(topicName: String): F[GenericTopicPartition[Option[KafkaOffset]]] =
    for {
      tps <- partitionsFor(topicName).map(_.topicPartitions)
      ret <- F.delay {
        consumer.endOffsets(tps.asJava).asScala.toMap.mapValues(Option(_).map(KafkaOffset(_)))
      }
    } yield GenericTopicPartition(ret)

  def offsetsForTimes(
    topicName: String,
    ts: NJTimestamp): F[GenericTopicPartition[Option[OffsetAndTimestamp]]] =
    for {
      tps <- partitionsFor(topicName).map(_.topicPartitions)
      ret <- F.delay {
        consumer.offsetsForTimes(tps.javaTimed(ts)).asScala.toMap.mapValues(Option(_))
      }
    } yield GenericTopicPartition(ret)

  def retrieveRecord(gtp: GenericTopicPartition[KafkaOffset])
    : F[GenericTopicPartition[Option[KafkaByteConsumerRecord]]] =
    F.delay {
      consumer.assign(gtp.topicPartitions.asJava)
      gtp.map {
        case (tp, offset) =>
          consumer.seek(tp, offset.value)
          consumer.poll(Duration.ofSeconds(15)).records(tp).asScala.toList.headOption
      }
    }

  def offsetRangeFor(
    topicName: String,
    dtr: NJDateTimeRange): F[GenericTopicPartition[KafkaOffsetRange]] =
    for {
      start <- dtr.start.fold(beginningOffsets(topicName))(
        offsetsForTimes(topicName, _).map(_.offsets))
      end <- dtr.end.fold(endOffsets(topicName))(offsetsForTimes(topicName, _).map(_.offsets))
    } yield start
      .combineWith(end)((_, _).mapN((f, s) => KafkaOffsetRange(f, s)))
      .flatten[KafkaOffsetRange]
}
