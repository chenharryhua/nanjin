package com.github.chenharryhua.nanjin.kafka.connector

import cats.Endo
import cats.data.{NonEmptyList, NonEmptySet, ReaderT}
import cats.effect.kernel.{Async, Resource}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.common.{HasProperties, UpdateConfig}
import com.github.chenharryhua.nanjin.datetime.DateTimeRange
import com.github.chenharryhua.nanjin.kafka.orderingTopicPartition
import fs2.Stream
import fs2.kafka.{AutoOffsetReset, CommittableConsumerRecord, ConsumerSettings, KafkaConsumer}
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

import java.time.Instant
import scala.collection.immutable.SortedSet

final class ConsumeKafka[F[_]: Async, K, V] private[kafka] (
  topicName: TopicName,
  consumerSettings: ConsumerSettings[F, K, V]
) extends ConsumerService[F, K, V] with UpdateConfig[ConsumerSettings[F, K, V], ConsumeKafka[F, K, V]]
    with HasProperties {

  /*
   * config
   */
  override def properties: Map[String, String] = consumerSettings.properties

  override def updateConfig(f: Endo[ConsumerSettings[F, K, V]]): ConsumeKafka[F, K, V] =
    new ConsumeKafka[F, K, V](topicName, f(consumerSettings))

  /*
   * client
   */

  def clientR: Resource[F, KafkaConsumer[F, K, V]] =
    KafkaConsumer.resource(consumerSettings)

  def clientS: Stream[F, KafkaConsumer[F, K, V]] =
    KafkaConsumer.stream(consumerSettings)

  /*
   * Records
   */

  def subscribe: Stream[F, CommittableConsumerRecord[F, K, V]] =
    clientS.evalTap(_.subscribe(NonEmptyList.one(topicName.name.value))).flatMap(_.stream)

  def assign: Stream[F, CommittableConsumerRecord[F, K, V]] =
    clientS.evalTap(_.assign(topicName.name.value)).flatMap(_.stream)

  def assign(partitionOffsets: Map[Int, Long]): Stream[F, CommittableConsumerRecord[F, K, V]] = {
    val topic_offset: Map[TopicPartition, Long] =
      partitionOffsets.map { case (p, o) => new TopicPartition(topicName.name.value, p) -> o }

    NonEmptySet.fromSet(SortedSet.from(topic_offset.keySet)) match {
      case None      => Stream.empty
      case Some(tps) =>
        KafkaConsumer
          .stream(consumerSettings.withAutoOffsetReset(AutoOffsetReset.None))
          .evalTap { c =>
            c.assign(tps) *>
              topic_offset.toList.traverse { case (p, o) => c.seek(p, o) }
          }
          .flatMap(_.stream)
    }
  }

  def assign(time: Instant): Stream[F, CommittableConsumerRecord[F, K, V]] =
    KafkaConsumer
      .stream(consumerSettings)
      .evalTap { c =>
        for {
          _ <- c.assign(topicName.name.value)
          partitions <- c.partitionsFor(topicName.name.value)
          tps = partitions.map { pi =>
            new TopicPartition(pi.topic(), pi.partition()) -> time.toEpochMilli
          }.toMap
          tpm <- c.offsetsForTimes(tps)
          _ <- tpm.toList.traverse { case (tp, oot) =>
            oot match {
              case Some(ot) => c.seek(tp, ot.offset())
              case None     => c.seekToEnd(NonEmptyList.one(tp))
            }
          }
        } yield ()
      }
      .flatMap(_.stream)

  /*
   * manual commit stream
   */

  def manualCommitStream: Stream[F, ManualCommitStream[F, K, V]] =
    KafkaConsumer
      .stream(consumerSettings.withEnableAutoCommit(false))
      .evalTap(_.subscribe(NonEmptyList.one(topicName.name.value)))
      .flatMap(kc =>
        kc.partitionsMapStream.map { pms =>
          new ManualCommitStream[F, K, V] {
            override def commitSync: ReaderT[F, Map[TopicPartition, OffsetAndMetadata], Unit] =
              ReaderT(kc.commitSync)

            override def commitAsync: ReaderT[F, Map[TopicPartition, OffsetAndMetadata], Unit] =
              ReaderT(kc.commitAsync)

            override def partitionsMapStream
              : Map[TopicPartition, Stream[F, CommittableConsumerRecord[F, K, V]]] =
              pms
          }
        })

  /*
   * Circumscribed Stream
   */

  private def circumscribed(
    or: Either[DateTimeRange, Map[Int, (Long, Long)]]): Stream[F, CircumscribedStream[F, K, V]] =
    for {
      kc <- KafkaConsumer.stream(consumerSettings.withEnableAutoCommit(false))
      ranges <- Stream.eval(utils.get_offset_range(kc, topicName, or))
      stream <-
        if (ranges.isEmpty) Stream.empty
        else {
          Stream.eval(utils.assign_offset_range(kc, ranges)) *>
            utils.circumscribed_stream(kc, ranges)
        }
    } yield stream

  def circumscribedStream(dateTimeRange: DateTimeRange): Stream[F, CircumscribedStream[F, K, V]] =
    circumscribed(Left(dateTimeRange))

  def circumscribedStream(partitionOffsets: Map[Int, (Long, Long)]): Stream[F, CircumscribedStream[F, K, V]] =
    circumscribed(Right(partitionOffsets))
}
