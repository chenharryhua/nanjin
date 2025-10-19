package com.github.chenharryhua.nanjin.kafka.connector

import cats.data.ReaderT
import cats.effect.Concurrent
import com.github.chenharryhua.nanjin.datetime.DateTimeRange
import com.github.chenharryhua.nanjin.kafka.{OffsetRange, PartitionRange, TopicPartitionMap}
import fs2.kafka.CommittableConsumerRecord
import fs2.{Chunk, Pipe, Stream}
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition

import java.time.Instant

/*
 * Stream which has a boundary
 */
trait CircumscribedStream[F[_], K, V] {
  def stopConsuming: F[Unit]

  def partitionsMapStream: Map[PartitionRange, Stream[F, CommittableConsumerRecord[F, K, V]]]

  final def stream(implicit F: Concurrent[F]): Stream[F, CommittableConsumerRecord[F, K, V]] =
    Stream.iterable(partitionsMapStream.values).parJoinUnbounded.onFinalize(stopConsuming)

  final def offsets: TopicPartitionMap[OffsetRange] =
    TopicPartitionMap(partitionsMapStream.keySet.map(pr => pr.topicPartition -> pr.offsetRange))
}

/*
 * Commit offset manually
 */
trait ManualCommitStream[F[_], K, V] {
  def commitSync: ReaderT[F, Map[TopicPartition, OffsetAndMetadata], Unit]
  def commitAsync: ReaderT[F, Map[TopicPartition, OffsetAndMetadata], Unit]

  def partitionsMapStream: Map[TopicPartition, Stream[F, CommittableConsumerRecord[F, K, V]]]

  final def stream(implicit F: Concurrent[F]): Stream[F, CommittableConsumerRecord[F, K, V]] =
    Stream.iterable(partitionsMapStream.values).parJoinUnbounded
}

/*
 * Consumer Service
 */
trait ConsumerService[F[_], K, V] {
  def subscribe: Stream[F, CommittableConsumerRecord[F, K, V]]

  def assign: Stream[F, CommittableConsumerRecord[F, K, V]]
  def assign(partitionOffsets: Map[Int, Long]): Stream[F, CommittableConsumerRecord[F, K, V]]
  def assign(time: Instant): Stream[F, CommittableConsumerRecord[F, K, V]]

  def manualCommitStream: Stream[F, ManualCommitStream[F, K, V]]

  def circumscribedStream(dateTimeRange: DateTimeRange): Stream[F, CircumscribedStream[F, K, V]]
  def circumscribedStream(partitionOffsets: Map[Int, (Long, Long)]): Stream[F, CircumscribedStream[F, K, V]]
}

/*
 * Producer Service
 */

trait ProducerService[F[_], A] {
  def sink: Pipe[F, A, Chunk[RecordMetadata]]
  def produceOne(record: A): F[RecordMetadata]
}
