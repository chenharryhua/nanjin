package com.github.chenharryhua.nanjin.kafka.connector

import cats.Foldable
import cats.data.ReaderT
import cats.effect.kernel.{Concurrent, Resource}
import com.github.chenharryhua.nanjin.kafka.{OffsetRange, PartitionRange, TopicPartitionMap}
import fs2.kafka.{CommittableConsumerRecord, KafkaProducer, ProducerRecord, ProducerResult}
import fs2.{Pipe, Stream}
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition

/*
 * Stream which has a boundary
 */
trait CircumscribedStream[F[_], K, V] {
  def stopConsuming: F[Unit]

  def rangedStreams: Map[PartitionRange, Stream[F, CommittableConsumerRecord[F, K, V]]]

  final def stream(using F: Concurrent[F]): Stream[F, CommittableConsumerRecord[F, K, V]] =
    Stream.iterable(rangedStreams.values).parJoinUnbounded.onFinalize(stopConsuming)

  final def offsets: TopicPartitionMap[OffsetRange] =
    TopicPartitionMap(rangedStreams.keySet.map(pr => pr.topicPartition -> pr.offsetRange))
}

/*
 * Commit offset manually
 */
trait ManualCommitStream[F[_], K, V] {
  def commitSync: ReaderT[F, Map[TopicPartition, OffsetAndMetadata], Unit]
  def commitAsync: ReaderT[F, Map[TopicPartition, OffsetAndMetadata], Unit]

  def partitionsMapStream: TopicPartitionMap[Stream[F, CommittableConsumerRecord[F, K, V]]]

  final def stream(using F: Concurrent[F]): Stream[F, CommittableConsumerRecord[F, K, V]] =
    Stream.iterable(partitionsMapStream.treeMap.values).parJoinUnbounded
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
