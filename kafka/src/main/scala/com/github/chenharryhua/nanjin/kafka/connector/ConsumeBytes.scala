package com.github.chenharryhua.nanjin.kafka.connector

import cats.Endo
import cats.data.{NonEmptyList, NonEmptySet, ReaderT}
import cats.effect.kernel.{Async, Resource}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.{HasProperties, UpdateConfig}
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.datetime.DateTimeRange
import com.github.chenharryhua.nanjin.kafka.orderingTopicPartition
import fs2.Stream
import fs2.kafka.{AutoOffsetReset, CommittableConsumerRecord, ConsumerSettings, KafkaConsumer}
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

import java.time.Instant
import scala.collection.immutable.SortedSet

final class ConsumeBytes[F[_]: Async] private[kafka] (
  topicName: TopicName,
  consumerSettings: ConsumerSettings[F, Array[Byte], Array[Byte]]
) extends ConsumerService[F, Array[Byte], Array[Byte]]
    with UpdateConfig[ConsumerSettings[F, Array[Byte], Array[Byte]], ConsumeBytes[F]] with HasProperties {

  /*
   * config
   */
  override lazy val properties: Map[String, String] = consumerSettings.properties

  override def updateConfig(f: Endo[ConsumerSettings[F, Array[Byte], Array[Byte]]]): ConsumeBytes[F] =
    new ConsumeBytes[F](topicName, f(consumerSettings))

  /*
   * client
   */

  lazy val clientR: Resource[F, KafkaConsumer[F, Array[Byte], Array[Byte]]] =
    KafkaConsumer.resource(consumerSettings)

  lazy val clientS: Stream[F, KafkaConsumer[F, Array[Byte], Array[Byte]]] =
    KafkaConsumer.stream(consumerSettings)

  /*
   * Records
   */

  lazy val subscribe: Stream[F, CommittableConsumerRecord[F, Array[Byte], Array[Byte]]] =
    clientS.evalTap(_.subscribe(NonEmptyList.one(topicName.name.value))).flatMap(_.stream)

  lazy val assign: Stream[F, CommittableConsumerRecord[F, Array[Byte], Array[Byte]]] =
    clientS.evalTap(_.assign(topicName.name.value)).flatMap(_.stream)

  def assign(
    partitionOffsets: Map[Int, Long]): Stream[F, CommittableConsumerRecord[F, Array[Byte], Array[Byte]]] = {
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

  def assign(time: Instant): Stream[F, CommittableConsumerRecord[F, Array[Byte], Array[Byte]]] =
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

  lazy val manualCommitStream: Stream[F, ManualCommitStream[F, Array[Byte], Array[Byte]]] =
    KafkaConsumer
      .stream(consumerSettings.withEnableAutoCommit(false))
      .evalTap(_.subscribe(NonEmptyList.one(topicName.name.value)))
      .flatMap(kc =>
        kc.partitionsMapStream.map { pms =>
          new ManualCommitStream[F, Array[Byte], Array[Byte]] {
            override def commitSync: ReaderT[F, Map[TopicPartition, OffsetAndMetadata], Unit] =
              ReaderT(kc.commitSync)

            override def commitAsync: ReaderT[F, Map[TopicPartition, OffsetAndMetadata], Unit] =
              ReaderT(kc.commitAsync)

            override def partitionsMapStream
              : Map[TopicPartition, Stream[F, CommittableConsumerRecord[F, Array[Byte], Array[Byte]]]] =
              pms
          }
        })

  /*
   * Circumscribed Stream
   */

  private def circumscribed(or: Either[DateTimeRange, Map[Int, (Long, Long)]])
    : Stream[F, CircumscribedStream[F, Array[Byte], Array[Byte]]] =
    for {
      kc <- KafkaConsumer.stream(consumerSettings.withEnableAutoCommit(false))
      ranges <- Stream.eval(topic_utils.get_offset_range(kc, topicName, or))
      stream <-
        if (ranges.isEmpty) Stream.empty
        else {
          Stream.eval(topic_utils.assign_offset_range(kc, ranges)) *>
            topic_utils.circumscribed_stream(kc, ranges)
        }
    } yield stream

  def circumscribedStream(
    dateTimeRange: DateTimeRange): Stream[F, CircumscribedStream[F, Array[Byte], Array[Byte]]] =
    circumscribed(Left(dateTimeRange))

  def circumscribedStream(
    partitionOffsets: Map[Int, (Long, Long)]): Stream[F, CircumscribedStream[F, Array[Byte], Array[Byte]]] =
    circumscribed(Right(partitionOffsets))
}
