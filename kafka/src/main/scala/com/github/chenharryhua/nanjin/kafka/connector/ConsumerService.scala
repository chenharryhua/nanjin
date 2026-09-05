package com.github.chenharryhua.nanjin.kafka.connector

import cats.data.NonEmptyList
import cats.effect.kernel.Sync
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import cats.syntax.apply.given
import cats.syntax.traverse.given
import com.github.chenharryhua.nanjin.datetime.DateTimeRange
import com.github.chenharryhua.nanjin.kafka.{EmptyTopicPartitionMap, TopicName, TopicPartitionMap}
import fs2.Stream
import fs2.kafka.CommittableConsumerRecord
import fs2.kafka.consumer.{KafkaAssignment, KafkaOffsets, KafkaTopics}
import org.apache.kafka.common.TopicPartition

import java.time.Instant

/** A Kafka consumer service providing bounded, manual-commit, and unbounded consumption modes.
  *
  * Implementations create consumer streams assigned to specific topic partitions and offset ranges. All
  * streams yield fs2 `CommittableConsumerRecord[F, K, V]`; the `K`/`V` payload depends on the implementation
  * (typed via deserializers in `ConsumeKafka`, or `Either[PullError, Record]` in `ConsumeGenericRecord`). Use
  * `KafkaContext.consume(topic)` (or `consumeGenericRecord`) to obtain a concrete instance.
  *
  * The modes differ in how partitions are acquired and where consumption stops:
  *   - `subscribe` uses Kafka's consumer-group subscription (partitions are assigned dynamically and may
  *     rebalance); the other modes use manual `assign` (fixed partitions, no group rebalancing).
  *   - `manualCommitStream` disables auto-commit and hands the caller explicit `commitSync`/`commitAsync`.
  *   - `circumscribedStream` is bounded: it reads only a resolved offset range and then completes.
  */
trait ConsumerService[F[_], K, V] {

  /** Assign the topic's partitions and seek each to the earliest offset at or after `time` (seeking to the
    * end when a partition has no such offset). Shared helper for time-based assignment.
    */
  protected def assignByTime(
    kc: KafkaAssignment[F] & KafkaTopics[F] & KafkaOffsets[F],
    tn: TopicName,
    time: Instant)(using Sync[F]): F[Unit] =
    for {
      _ <- kc.assign(tn.value)
      partitions <- kc.partitionsFor(tn.value)
      tps = partitions.map { pi =>
        new TopicPartition(pi.topic(), pi.partition()) -> time.toEpochMilli
      }.toMap
      tpm <- kc.offsetsForTimes(tps)
      _ <- tpm.toList.traverse { case (tp, oot) =>
        oot match {
          case Some(ot) => kc.seek(tp, ot.offset())
          case None     => kc.seekToEnd(NonEmptyList.one(tp))
        }
      }
    } yield ()

  /** Assign the partitions named by `map` (partition -> starting offset) and seek each to its offset. Raises
    * `EmptyTopicPartitionMap` if the map is empty. Shared helper for explicit partition/offset assignment.
    */
  protected def assignByMap(
    kc: KafkaAssignment[F] & KafkaTopics[F] & KafkaOffsets[F],
    tn: TopicName,
    map: Map[Int, Long])(using F: Sync[F]): F[Unit] = {
    val tpm = TopicPartitionMap(map.map { case (p, o) => new TopicPartition(tn.value, p) -> o })

    tpm.nonEmptyKeySet match {
      case Some(value) => kc.assign(value) <* tpm.toList.traverse { case (p, o) => kc.seek(p, o) }
      case None        => F.raiseError(EmptyTopicPartitionMap(tn))
    }
  }

  /** Subscribe to the topic as part of a consumer group. Partitions are assigned dynamically and may
    * rebalance; offsets are auto-committed unless the settings disable it. Unbounded.
    */
  def subscribe: Stream[F, CommittableConsumerRecord[F, K, V]]

  /** Like `subscribe`, but exposes the per-partition streams separately (keyed by `TopicPartition`) instead
    * of flattening them, letting the caller process partitions independently or in parallel.
    */
  def partitionsMapStream: Stream[F, TopicPartitionMap[Stream[F, CommittableConsumerRecord[F, K, V]]]]

  /** Manually assign all of the topic's partitions (no consumer group, no rebalancing) and stream from the
    * current position. Unbounded.
    */
  def assign: Stream[F, CommittableConsumerRecord[F, K, V]]

  /** Manually assign the given partitions and seek each to its starting offset (partition -> offset). With no
    * group rebalancing. Unbounded.
    */
  def assign(partitionOffsets: Map[Int, Long]): Stream[F, CommittableConsumerRecord[F, K, V]]

  /** Manually assign the topic's partitions and seek each to the first offset at or after `time`. Unbounded.
    */
  def assign(time: Instant): Stream[F, CommittableConsumerRecord[F, K, V]]

  /** Subscribe with auto-commit disabled, exposing per-partition streams together with explicit
    * `commitSync`/`commitAsync` so the caller controls when offsets are committed.
    */
  def manualCommitStream: Stream[F, ManualCommitStream[F, K, V]]

  /** Bounded consumption over the offset range covered by `dateTimeRange`: reads from the first offset at or
    * after the start until the range's end, then completes. Empty range yields an empty stream.
    */
  def circumscribedStream(dateTimeRange: DateTimeRange): Stream[F, CircumscribedStream[F, K, V]]

  /** Bounded consumption over explicit per-partition ranges (partition -> (start, end) offsets): reads each
    * partition's `[start, end)` and then completes. Empty map yields an empty stream.
    */
  def circumscribedStream(partitionOffsets: Map[Int, (Long, Long)]): Stream[F, CircumscribedStream[F, K, V]]
}
