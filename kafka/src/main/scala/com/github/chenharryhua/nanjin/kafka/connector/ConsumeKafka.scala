package com.github.chenharryhua.nanjin.kafka.connector

import cats.Endo
import cats.data.{NonEmptyList, ReaderT}
import cats.effect.kernel.Async
import com.github.chenharryhua.nanjin.common.{HasProperties, UpdateConfig}
import com.github.chenharryhua.nanjin.datetime.DateTimeRange
import com.github.chenharryhua.nanjin.kafka.{TopicName, TopicPartitionMap}
import fs2.Stream
import fs2.kafka.{AutoOffsetReset, CommittableConsumerRecord, ConsumerSettings, KafkaConsumer}
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

import java.time.Instant

/** A `ConsumerService` that consumes a topic into typed `K`/`V` records using the deserializers carried by
  * `consumerSettings`.
  *
  * Unlike `ConsumeGenericRecord` (which reads raw bytes and decodes to Avro `GenericRecord`), this connector
  * deserializes directly into the key/value types the settings were built with, so the element type is the
  * plain fs2 `CommittableConsumerRecord[F, K, V]` with no `PullError` wrapper. It inherits all consumption
  * modes from `ConsumerService`, subscribe, assign (by nothing, by partition/offset map, or by time),
  * manual-commit, and bounded ("circumscribed") streams. Obtain an instance via
  * `KafkaContext.consume(topic)`.
  */
final class ConsumeKafka[F[_]: Async, K, V] private[kafka] (
  topicName: TopicName,
  consumerSettings: ConsumerSettings[F, K, V]
) extends ConsumerService[F, K, V] with UpdateConfig[ConsumerSettings[F, K, V], ConsumeKafka[F, K, V]]
    with HasProperties {

  /*
   * config
   */
  override lazy val properties: Map[String, String] = consumerSettings.properties

  /** Return a copy with the underlying consumer settings transformed by `f`. */
  override def updateConfig(f: Endo[ConsumerSettings[F, K, V]]): ConsumeKafka[F, K, V] =
    new ConsumeKafka[F, K, V](topicName, f(consumerSettings))

  /** Shared consumer resource stream for the subscribe/assign modes. */
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

  /** Shared implementation of the bounded streams: resolve the offset range (from a date-time range or an
    * explicit per-partition range), assign it, and emit a bounded stream; an empty range yields an empty
    * stream. Auto-commit is disabled for these.
    */
  private def circumscribed(
    or: Either[DateTimeRange, Map[Int, (Long, Long)]]): Stream[F, CircumscribedStream[F, K, V]] =
    for {
      kc <- KafkaConsumer.stream(consumerSettings.withEnableAutoCommit(false))
      ranges <- Stream.eval(topicUtils.get_offset_range(kc, topicName, or))
      isAssigned <- Stream.eval(topicUtils.assign_offset_range(kc, ranges))
      stream <- if isAssigned then topicUtils.circumscribed_stream(kc, ranges) else Stream.empty
    } yield stream

  override def circumscribedStream(dateTimeRange: DateTimeRange): Stream[F, CircumscribedStream[F, K, V]] =
    circumscribed(Left(dateTimeRange))

  override def circumscribedStream(
    partitionOffsets: Map[Int, (Long, Long)]): Stream[F, CircumscribedStream[F, K, V]] =
    circumscribed(Right(partitionOffsets))
}
