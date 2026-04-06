package com.github.chenharryhua.nanjin.kafka.connector

import cats.Endo
import cats.data.{NonEmptyList, ReaderT}
import cats.effect.kernel.Async
import cats.syntax.apply.given
import com.github.chenharryhua.nanjin.common.{HasProperties, UpdateConfig}
import com.github.chenharryhua.nanjin.datetime.DateTimeRange
import com.github.chenharryhua.nanjin.kafka.{TopicName, TopicPartitionMap}
import fs2.Stream
import fs2.kafka.{AutoOffsetReset, CommittableConsumerRecord, ConsumerSettings, KafkaConsumer}
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

import java.time.Instant

final class ConsumeKafka[F[_]: Async, K, V] private[kafka] (
  topicName: TopicName,
  consumerSettings: ConsumerSettings[F, K, V]
) extends ConsumerService[F, K, V] with UpdateConfig[ConsumerSettings[F, K, V], ConsumeKafka[F, K, V]]
    with HasProperties {

  /*
   * config
   */
  override lazy val properties: Map[String, String] = consumerSettings.properties

  override def updateConfig(f: Endo[ConsumerSettings[F, K, V]]): ConsumeKafka[F, K, V] =
    new ConsumeKafka[F, K, V](topicName, f(consumerSettings))

  private lazy val clientS: Stream[F, KafkaConsumer[F, K, V]] =
    KafkaConsumer.stream(consumerSettings)

  /*
   * Records
   */

  override lazy val subscribe: Stream[F, CommittableConsumerRecord[F, K, V]] =
    clientS.evalTap(_.subscribe(NonEmptyList.one(topicName.value))).flatMap(_.stream)

  override lazy val partitionsMapStream
    : Stream[F, TopicPartitionMap[Stream[F, CommittableConsumerRecord[F, K, V]]]] =
    clientS.evalTap(_.subscribe(NonEmptyList.one(topicName.value)))
      .flatMap(_.partitionsMapStream.map(TopicPartitionMap(_)))

  override lazy val assign: Stream[F, CommittableConsumerRecord[F, K, V]] =
    clientS.evalTap(_.assign(topicName.value)).flatMap(_.stream)

  override def assign(partitionOffsets: Map[Int, Long]): Stream[F, CommittableConsumerRecord[F, K, V]] =
    KafkaConsumer
      .stream(consumerSettings.withAutoOffsetReset(AutoOffsetReset.None))
      .evalTap(assignByMap(_, topicName, partitionOffsets))
      .flatMap(_.stream)

  override def assign(time: Instant): Stream[F, CommittableConsumerRecord[F, K, V]] =
    KafkaConsumer
      .stream(consumerSettings)
      .evalTap(assignByTime(_, topicName, time))
      .flatMap(_.stream)

  /*
   * manual commit stream
   */

  override lazy val manualCommitStream: Stream[F, ManualCommitStream[F, K, V]] =
    KafkaConsumer
      .stream(consumerSettings.withEnableAutoCommit(false))
      .evalTap(_.subscribe(NonEmptyList.one(topicName.value)))
      .flatMap(kc =>
        kc.partitionsMapStream.map { pms =>
          new ManualCommitStream[F, K, V] {
            override def commitSync: ReaderT[F, Map[TopicPartition, OffsetAndMetadata], Unit] =
              ReaderT(kc.commitSync)

            override def commitAsync: ReaderT[F, Map[TopicPartition, OffsetAndMetadata], Unit] =
              ReaderT(kc.commitAsync)

            override def partitionsMapStream
              : TopicPartitionMap[Stream[F, CommittableConsumerRecord[F, K, V]]] =
              TopicPartitionMap(pms)
          }
        })

  /*
   * Circumscribed Stream
   */

  private def circumscribed(
    or: Either[DateTimeRange, Map[Int, (Long, Long)]]): Stream[F, CircumscribedStream[F, K, V]] =
    for {
      kc <- KafkaConsumer.stream(consumerSettings.withEnableAutoCommit(false))
      ranges <- Stream.eval(topicUtils.get_offset_range(kc, topicName, or))
      stream <-
        if (ranges.isEmpty) Stream.empty
        else {
          Stream.eval(topicUtils.assign_offset_range(kc, ranges)) *>
            topicUtils.circumscribed_stream(kc, ranges)
        }
    } yield stream

  override def circumscribedStream(dateTimeRange: DateTimeRange): Stream[F, CircumscribedStream[F, K, V]] =
    circumscribed(Left(dateTimeRange))

  override def circumscribedStream(
    partitionOffsets: Map[Int, (Long, Long)]): Stream[F, CircumscribedStream[F, K, V]] =
    circumscribed(Right(partitionOffsets))
}
