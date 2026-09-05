package com.github.chenharryhua.nanjin.kafka.connector

import cats.Foldable
import cats.data.ReaderT
import cats.effect.kernel.{Concurrent, Resource}
import com.github.chenharryhua.nanjin.kafka.{OffsetRange, PartitionRange, TopicPartitionMap}
import fs2.kafka.{CommittableConsumerRecord, KafkaProducer, ProducerRecord, ProducerRecords, ProducerResult}
import fs2.{Pipe, Stream}
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition

/** A bounded Kafka consumer stream that reads a fixed offset range per partition.
  *
  * The stream terminates automatically when all partitions have been consumed up to their configured end
  * offsets. Call `stopConsuming` to force an early shutdown.
  */
trait CircumscribedStream[F[_], K, V] {
  def stopConsuming: F[Unit]

  def rangedStreams: Map[PartitionRange, Stream[F, CommittableConsumerRecord[F, K, V]]]

  final def stream(using F: Concurrent[F]): Stream[F, CommittableConsumerRecord[F, K, V]] =
    Stream.iterable(rangedStreams.values).parJoinUnbounded.onFinalize(stopConsuming)

  final def offsets: TopicPartitionMap[OffsetRange] =
    TopicPartitionMap(rangedStreams.keySet.map(pr => pr.topicPartition -> pr.offsetRange))
}

/** A Kafka consumer stream with manual offset commit control.
  *
  * Records are delivered without auto-commit; the caller is responsible for committing offsets via
  * `commitSync` or `commitAsync`.
  */
trait ManualCommitStream[F[_], K, V] {
  def commitSync: ReaderT[F, Map[TopicPartition, OffsetAndMetadata], Unit]
  def commitAsync: ReaderT[F, Map[TopicPartition, OffsetAndMetadata], Unit]

  def partitionsMapStream: TopicPartitionMap[Stream[F, CommittableConsumerRecord[F, K, V]]]

  final def stream(using F: Concurrent[F]): Stream[F, CommittableConsumerRecord[F, K, V]] =
    Stream.iterable(partitionsMapStream.treeMap.values).parJoinUnbounded
}

/** A Kafka producer with resource-managed client lifecycle and convenience sinks.
  *
  * Acquire the producer via `clientR` (resource) or `clientS` (stream), or pipe records directly through
  * `sink` or `pairSink`.
  */
trait ProducerService[F[_], K, V] {

  /** The producer as a `Resource`: acquired on `use`, closed on release. Prefer for one-shot sends. */
  def clientR: Resource[F, KafkaProducer[F, K, V]]

  /** The producer as a `Stream`: emits a single producer whose lifecycle is bound to the stream. Prefer when
    * composing with other fs2 stages.
    */
  def clientS: Stream[F, KafkaProducer[F, K, V]]

  /** Pipe that produces a stream of `(key, value)` pairs to the configured topic, batching by chunk and
    * running the sends in parallel.
    */
  def pairSink: Pipe[F, (K, V), ProducerResult[K, V]]

  /** Pipe that produces a stream of fully-formed `ProducerRecord`s (letting the caller set partition,
    * timestamp, headers, or a different topic).
    */
  def sink: Pipe[F, ProducerRecord[K, V], ProducerResult[K, V]]

  /** Pipe that produces pre-batched `ProducerRecords`, one Kafka batch per element, sends run in parallel. */
  def chunkSink: Pipe[F, ProducerRecords[K, V], ProducerResult[K, V]]

  /** Produce a single `(key, value)` to the configured topic and return its `RecordMetadata`. */
  def produceOne(k: K, v: V): F[RecordMetadata]

  /** Produce a single fully-formed `ProducerRecord` and return its `RecordMetadata`. */
  def produceOne(record: ProducerRecord[K, V]): F[RecordMetadata]

  /** Produce a batch of `(key, value)` pairs (any `Foldable`) to the configured topic in one call. */
  def produce[G[_]: Foldable](kvs: G[(K, V)]): F[ProducerResult[K, V]]

}
