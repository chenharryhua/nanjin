package com.github.chenharryhua.nanjin.kafka

import java.time.Duration

import cats.effect.Sync
import cats.implicits._
import com.github.chenharryhua.nanjin.datetime.{NJDateTimeRange, NJTimestamp}
import fs2.kafka.{KafkaByteConsumer, KafkaByteConsumerRecord}
import org.apache.kafka.clients.consumer.{ConsumerRecord, OffsetAndTimestamp}
import org.apache.kafka.common.{PartitionInfo, TopicPartition}

import scala.collection.JavaConverters._

final class ConsumerWrapper[F[_]](consumer: KafkaByteConsumer)(implicit F: Sync[F]) {

  def partitionsFor(topicName: String): F[List[PartitionInfo]] =
    F.delay(consumer.partitionsFor(topicName).asScala.toList.mapFilter(Option(_)))

  def beginningOffsets(topicName: String): F[GenericTopicPartition[Option[KafkaOffset]]] =
    for {
      tps <- partitionsFor(topicName).map(_.map(info =>
        new TopicPartition(topicName, info.partition)))
      ret <- F.delay(
        consumer
          .beginningOffsets(tps.asJava)
          .asScala
          .toMap
          .mapValues(Option(_).map(KafkaOffset(_))))
    } yield GenericTopicPartition(ret)

  def endOffsets(topicName: String): F[GenericTopicPartition[Option[KafkaOffset]]] =
    for {
      tps <- partitionsFor(topicName).map(_.map(info =>
        new TopicPartition(topicName, info.partition)))
      ret <- F.delay(
        consumer.endOffsets(tps.asJava).asScala.toMap.mapValues(Option(_).map(KafkaOffset(_))))
    } yield GenericTopicPartition(ret)

  def offsetsForTimes(
    topicName: String,
    ts: NJTimestamp): F[GenericTopicPartition[Option[OffsetAndTimestamp]]] =
    for {
      tps <- partitionsFor(topicName).map(lpi =>
        ListOfTopicPartitions(lpi.map(info => new TopicPartition(topicName, info.partition))))
      ret <- F.delay {
        consumer.offsetsForTimes(tps.javaTimed(ts)).asScala.toMap.mapValues(Option(_))
      }
    } yield GenericTopicPartition(ret)

  def retrieveRecord(gtp: GenericTopicPartition[KafkaOffset])
    : F[GenericTopicPartition[Option[KafkaByteConsumerRecord]]] =
    F.delay {
      val tps = gtp.topicPartitions.value
      consumer.assign(tps.asJava)
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
        offsetsForTimes(topicName, _).map(_.mapValues(_.map(x => KafkaOffset(x.offset)))))
      end <- dtr.end.fold(endOffsets(topicName))(
        offsetsForTimes(topicName, _).map(_.mapValues(_.map(x => KafkaOffset(x.offset)))))
    } yield start
      .combineWith(end)((_, _).mapN((f, s) => KafkaOffsetRange(f, s)))
      .flatten[KafkaOffsetRange]

      
}
