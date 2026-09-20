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
  * offsets. Call `stopConsuming` to stop early and let buffered records drain.
  */
trait CircumscribedStream[F[_], K, V] {

  /** Signal the underlying consumer to stop fetching new records. Already-fetched records continue to be
    * emitted and the stream then completes gracefully; it does not abort in flight or close the consumer.
    * Invoked automatically as the finalizer of `stream`.
    */
  def stopConsuming: F[Unit]

  /** One bounded stream per partition range; each ends when its partition reaches its end offset. */
  def rangedStreams: Map[PartitionRange, Stream[F, CommittableConsumerRecord[F, K, V]]]

  /** All per-partition ranged streams merged, running in parallel, finalizing with `stopConsuming`. The
    * merged stream terminates once every partition has been consumed to its end offset.
    */
  final def stream(using F: Concurrent[F]): Stream[F, CommittableConsumerRecord[F, K, V]] =
    Stream.iterable(rangedStreams.values).parJoinUnbounded.onFinalize(stopConsuming)

  /** The offset range being consumed per partition. */
  final def offsets: TopicPartitionMap[OffsetRange] =
    TopicPartitionMap(rangedStreams.keySet.map(pr => pr.topicPartition -> pr.offsetRange))
}

/** A Kafka consumer stream with manual offset commit control.
  *
  * Records are delivered without auto-commit; the caller is responsible for committing offsets via
  * `commitSync` or `commitAsync`.
  */
trait ManualCommitStream[F[_], K, V] {

  /** Synchronously commit the supplied offsets, blocking until the broker acknowledges. */
  def commitSync: ReaderT[F, Map[TopicPartition, OffsetAndMetadata], Unit]

  /** Asynchronously commit the supplied offsets, returning once the request is enqueued. */
  def commitAsync: ReaderT[F, Map[TopicPartition, OffsetAndMetadata], Unit]

  /** One stream per partition; records carry offset information but are not auto-committed. */
  def partitionsMapStream: TopicPartitionMap[Stream[F, CommittableConsumerRecord[F, K, V]]]

  /** All per-partition streams merged, running in parallel. Offsets must be committed by the caller via
    * `commitSync`/`commitAsync`.
    */
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
