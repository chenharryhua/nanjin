package com.github.chenharryhua.nanjin.kafka.connector

import cats.Foldable
import cats.data.ReaderT
import cats.effect.Concurrent
import cats.effect.kernel.Resource
import com.github.chenharryhua.nanjin.datetime.DateTimeRange
import com.github.chenharryhua.nanjin.kafka.{OffsetRange, PartitionRange, TopicPartitionMap}
import fs2.kafka.{CommittableConsumerRecord, KafkaProducer, ProducerRecord, ProducerResult}
import fs2.{Pipe, Stream}
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

  final def stream(using F: Concurrent[F]): Stream[F, CommittableConsumerRecord[F, K, V]] =
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

  def partitionsMapStream: TopicPartitionMap[Stream[F, CommittableConsumerRecord[F, K, V]]]

  final def stream(using F: Concurrent[F]): Stream[F, CommittableConsumerRecord[F, K, V]] =
    Stream.iterable(partitionsMapStream.value.values).parJoinUnbounded
}

/*
 * Consumer Service
 */
trait ConsumerService[F[_], K, V] {
  def subscribe: Stream[F, CommittableConsumerRecord[F, K, V]]

  def partitionsMapStream: Stream[F, TopicPartitionMap[Stream[F, CommittableConsumerRecord[F, K, V]]]]

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

trait ProducerService[F[_], K, V] {
  def clientR: Resource[F, KafkaProducer.Metrics[F, K, V]]
  def clientS: Stream[F, KafkaProducer.Metrics[F, K, V]]

  def pairSink: Pipe[F, (K, V), ProducerResult[K, V]]
  def sink: Pipe[F, ProducerRecord[K, V], ProducerResult[K, V]]

  def produceOne(k: K, v: V): F[RecordMetadata]
  def produceOne(record: ProducerRecord[K, V]): F[RecordMetadata]
  def produce[G[_]: Foldable](kvs: G[(K, V)]): F[ProducerResult[K, V]]

  def transactional(transactionalId: String): KafkaTransactional[F, K, V]
}
