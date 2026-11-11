package com.github.chenharryhua.nanjin.kafka.admins

import cats.data.Kleisli
import cats.effect.kernel.{Resource, Sync}
import cats.mtl.Ask
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import cats.syntax.functorFilter.given
import cats.syntax.traverse.given
import com.github.chenharryhua.nanjin.datetime.DateTimeRange
import com.github.chenharryhua.nanjin.kafka.{
  makePureConsumer,
  Offset,
  OffsetRange,
  Partition,
  PureConsumerSettings,
  TopicName,
  TopicPartitionList,
  TopicPartitionMap,
  given
}
import fs2.kafka.KafkaByteConsumer
import org.apache.kafka.clients.consumer.{ConsumerRecord, OffsetAndMetadata}
import org.apache.kafka.common.{Metric, MetricName, TopicPartition}

import java.time.Instant
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.jdk.CollectionConverters.*
import scala.jdk.DurationConverters.ScalaDurationOps

/** Low-level read operations on a Kafka consumer for a single topic.
  *
  * Provides partition discovery, offset queries (beginning, end, by timestamp), record retrieval, and
  * synchronous offset commits.
  *
  * Every operation is a synchronous Kafka client call, so each runs on the blocking pool and holds the
  * underlying consumer's monitor while it does: a `KafkaConsumer` is single-threaded and raises
  * `ConcurrentModificationException` when a second thread touches it, so concurrent callers have to be
  * serialized. The monitor is held per client call, not per method, so two operations issued concurrently
  * each complete safely, but a method composed of several client calls is not atomic against another caller's
  * calls.
  */
sealed trait KafkaConsumerOps[F[_]] {

  /** The partitions of the topic. */
  def partitionsFor: F[TopicPartitionList]

  /** The earliest available offset per partition (`None` where unavailable). */
  def beginningOffsets: F[TopicPartitionMap[Option[Offset]]]

  /** The next-to-be-produced (end) offset per partition (`None` where unavailable). */
  def endOffsets: F[TopicPartitionMap[Option[Offset]]]

  /** The earliest offset per partition whose record timestamp is at or after `ts`. */
  def offsetsForTimes(ts: Instant): F[TopicPartitionMap[Option[Offset]]]

  /** Fetch the single record at `partition`/`offset`, or `None` if none arrives within `timeout`.
    *
    * The call assigns `partition` to the consumer, seeks to `offset`, and polls. The consumer's previous
    * assignment is restored before returning, but the fetch positions of those partitions are not: they are
    * unassigned for the duration of the call, which discards them. `None` means the poll returned nothing,
    * which a `timeout` too short to complete the fetch produces just as an absent record does.
    */
  def retrieveRecord(
    partition: Partition,
    offset: Offset,
    timeout: FiniteDuration): F[Option[ConsumerRecord[Array[Byte], Array[Byte]]]]

  /** As `retrieveRecord`, polling for up to 15 seconds. */
  final def retrieveRecord(
    partition: Partition,
    offset: Offset): F[Option[ConsumerRecord[Array[Byte], Array[Byte]]]] =
    retrieveRecord(partition, offset, 15.seconds)

  /** Synchronously commit the given offsets for this consumer group. */
  def commitSync(offsets: Map[TopicPartition, OffsetAndMetadata]): F[Unit]

  /** The underlying consumer's metrics. */
  def metrics: F[Map[MetricName, Metric]]
}

private object KafkaConsumerOps {

  def apply[F[_]](topicName: TopicName)(using Sync[F], Ask[F, KafkaByteConsumer]): KafkaConsumerOps[F] =
    new KafkaPrimitiveConsumerApiImpl[F](topicName)

  final private class KafkaPrimitiveConsumerApiImpl[F[_]](topicName: TopicName)(using
    F: Sync[F],
    kbc: Ask[F, KafkaByteConsumer]
  ) extends KafkaConsumerOps[F] {

    /** Run one Kafka client call on the blocking pool, holding the consumer's monitor for its duration.
      *
      * The monitor is the consumer object itself rather than a field of this class, so instances that wrap
      * the same consumer (as `KafkaContext.ungroup` creates, one per topic name) exclude each other.
      */
    private def on_consumer[A](f: KafkaByteConsumer => A): F[A] =
      kbc.ask.flatMap(c => F.blocking(c.synchronized(f(c))))

    override val partitionsFor: F[TopicPartitionList] =
      on_consumer { c =>
        val ret: List[TopicPartition] = c
          .partitionsFor(topicName.value)
          .asScala
          .toList
          .mapFilter(Option(_))
          .map(info => new TopicPartition(topicName.value, info.partition))
        TopicPartitionList(ret)
      }

    override val beginningOffsets: F[TopicPartitionMap[Option[Offset]]] =
      for {
        tps <- partitionsFor
        ret <- on_consumer(_.beginningOffsets(tps.javaList).asScala)
      } yield TopicPartitionMap(ret.map { case (k, v) => k -> Option(v).map(Offset(_)) })

    override val endOffsets: F[TopicPartitionMap[Option[Offset]]] =
      for {
        tps <- partitionsFor
        ret <- on_consumer(_.endOffsets(tps.javaList).asScala)
      } yield TopicPartitionMap(ret.map { case (k, v) => k -> Option(v).map(Offset(_)) })

    override def offsetsForTimes(ts: Instant): F[TopicPartitionMap[Option[Offset]]] =
      for {
        tps <- partitionsFor
        ret <- on_consumer(_.offsetsForTimes(tps.javaTimed(ts)).asScala)
      } yield TopicPartitionMap(ret.map { case (k, v) => k -> Option(v).map(Offset(_)) })

    override def retrieveRecord(
      partition: Partition,
      offset: Offset,
      timeout: FiniteDuration): F[Option[ConsumerRecord[Array[Byte], Array[Byte]]]] =
      on_consumer { consumer =>
        val tp = new TopicPartition(topicName.value, partition.value)
        val previous = consumer.assignment()
        try {
          consumer.assign(List(tp).asJava)
          consumer.seek(tp, offset.value)
          consumer.poll(timeout.toJava).records(tp).asScala.toList.headOption
        } finally consumer.assign(previous)
      }

    override def commitSync(offsets: Map[TopicPartition, OffsetAndMetadata]): F[Unit] =
      on_consumer(_.commitSync(offsets.asJava))

    override val metrics: F[Map[MetricName, Metric]] =
      on_consumer(_.metrics().asScala.toMap)
  }
}

/** Extended consumer operations for point-in-time offset queries, offset range computation, record retrieval
  * at specific positions, and consumer group offset management.
  *
  * A `SnapshotConsumer` operates on a single topic and is typically obtained from `KafkaContext.admin`. It
  * runs every operation against one shared consumer, under the blocking and serialization rules described on
  * `KafkaConsumerOps`.
  */
sealed trait SnapshotConsumer[F[_]] extends KafkaConsumerOps[F] {

  /** Per-partition offset range bounding the wall-clock `DateTimeRange`: from the range start (or beginning
    * if unset) to the range end (or current end if unset). `None` for a partition with no data in range.
    */
  def offsetRangeFor(dtr: DateTimeRange): F[TopicPartitionMap[Option[OffsetRange]]]

  /** Per-partition offset range between the two timestamps. */
  def offsetRangeFor(start: Instant, end: Instant): F[TopicPartitionMap[Option[OffsetRange]]]

  /** Per-partition offset range covering all available data (beginning to end). */
  def offsetRangeForAll: F[TopicPartitionMap[Option[OffsetRange]]]

  /** The last record of each partition (skipping empty partitions), sorted by partition. */
  def retrieveLastRecords: F[List[ConsumerRecord[Array[Byte], Array[Byte]]]]

  /** The first record of each partition (skipping empty partitions), sorted by partition. */
  def retrieveFirstRecords: F[List[ConsumerRecord[Array[Byte], Array[Byte]]]]

  /** The first record at or after `ts` in each partition. */
  def retrieveRecordsForTimes(ts: Instant): F[List[ConsumerRecord[Array[Byte], Array[Byte]]]]

  /** Per-partition offset range from the first offset at or after `ts` to the current end. */
  def offsetRangeSince(ts: Instant): F[TopicPartitionMap[Option[OffsetRange]]]

  /** Commit this group's offsets to each partition's beginning. */
  def resetOffsetsToBegin: F[Unit]

  /** Commit this group's offsets to each partition's end. */
  def resetOffsetsToEnd: F[Unit]

  /** Commit this group's offsets to the first offset at or after `ts` in each partition. */
  def resetOffsetsForTimes(ts: Instant): F[Unit]
}

private[kafka] object SnapshotConsumer {

  def apply[F[_]: Sync](topicName: TopicName, cs: PureConsumerSettings): Resource[F, SnapshotConsumer[F]] =
    makePureConsumer(cs).map(consumer => new SnapshotConsumerImpl(topicName, consumer))
  def apply[F[_]: Sync](topicName: TopicName, consumer: KafkaByteConsumer): SnapshotConsumer[F] =
    new SnapshotConsumerImpl[F](topicName, consumer)

  final private class SnapshotConsumerImpl[F[_]: Sync](topicName: TopicName, consumer: KafkaByteConsumer)
      extends SnapshotConsumer[F] {

    private val kpc: KafkaConsumerOps[Kleisli[F, KafkaByteConsumer, *]] =
      KafkaConsumerOps[Kleisli[F, KafkaByteConsumer, *]](topicName)

    private def execute[A](r: Kleisli[F, KafkaByteConsumer, A]): F[A] =
      r.run(consumer)

    override def offsetRangeFor(dtr: DateTimeRange): F[TopicPartitionMap[Option[OffsetRange]]] =
      execute {
        for {
          from <- dtr.start.fold(kpc.beginningOffsets)(kpc.offsetsForTimes)
          end <- kpc.endOffsets
          to <- dtr.end.traverse(kpc.offsetsForTimes)
        } yield calculate.consumer_offsetRange(from, end, to)
      }

    override def offsetRangeFor(start: Instant, end: Instant): F[TopicPartitionMap[Option[OffsetRange]]] =
      execute {
        for {
          from <- kpc.offsetsForTimes(start)
          to <- kpc.offsetsForTimes(end)
        } yield calculate.consumer_offsetRange(from, to)
      }

    override val retrieveLastRecords: F[List[ConsumerRecord[Array[Byte], Array[Byte]]]] =
      execute {
        for {
          end <- kpc.endOffsets
          rec <- end.treeMap.toList.traverse { case (tp, of) =>
            of.filter(_.value > 0)
              .flatTraverse(offset => kpc.retrieveRecord(Partition(tp.partition), offset.asLast))
          }
        } yield rec.flatten.sortBy(_.partition())
      }

    override val retrieveFirstRecords: F[List[ConsumerRecord[Array[Byte], Array[Byte]]]] =
      execute {
        for {
          beg <- kpc.beginningOffsets
          rec <- beg.treeMap.toList.traverse { case (tp, of) =>
            of.flatTraverse(offset => kpc.retrieveRecord(Partition(tp.partition), offset))
          }
        } yield rec.flatten.sortBy(_.partition())
      }

    override def retrieveRecordsForTimes(ts: Instant): F[List[ConsumerRecord[Array[Byte], Array[Byte]]]] =
      execute {
        for {
          oft <- kpc.offsetsForTimes(ts)
          rec <- oft.treeMap.toList.traverse { case (tp, of) =>
            of.flatTraverse(offset => kpc.retrieveRecord(Partition(tp.partition), offset))
          }
        } yield rec.flatten
      }

    override val offsetRangeForAll: F[TopicPartitionMap[Option[OffsetRange]]] =
      execute {
        for {
          beg <- kpc.beginningOffsets
          end <- kpc.endOffsets
        } yield calculate.consumer_offsetRange(beg, end)
      }

    override def offsetRangeSince(ts: Instant): F[TopicPartitionMap[Option[OffsetRange]]] =
      execute {
        for {
          oft <- kpc.offsetsForTimes(ts)
          end <- kpc.endOffsets
        } yield calculate.consumer_offsetRange(oft, end)
      }

    override val partitionsFor: F[TopicPartitionList] =
      execute(kpc.partitionsFor)

    override val beginningOffsets: F[TopicPartitionMap[Option[Offset]]] =
      execute(kpc.beginningOffsets)

    override val endOffsets: F[TopicPartitionMap[Option[Offset]]] =
      execute(kpc.endOffsets)

    override def offsetsForTimes(ts: Instant): F[TopicPartitionMap[Option[Offset]]] =
      execute(kpc.offsetsForTimes(ts))

    override def retrieveRecord(
      partition: Partition,
      offset: Offset,
      timeout: FiniteDuration): F[Option[ConsumerRecord[Array[Byte], Array[Byte]]]] =
      execute(kpc.retrieveRecord(partition, offset, timeout))

    override def commitSync(offsets: Map[TopicPartition, OffsetAndMetadata]): F[Unit] =
      execute(kpc.commitSync(offsets))

    private def offsetsOf(
      offsets: TopicPartitionMap[Option[Offset]]): Map[TopicPartition, OffsetAndMetadata] =
      offsets.flatten.treeMap.map { case (k, offset) => k -> new OffsetAndMetadata(offset.value) }

    override val resetOffsetsToBegin: F[Unit] =
      execute(kpc.beginningOffsets.flatMap(x => kpc.commitSync(offsetsOf(x))))

    override val resetOffsetsToEnd: F[Unit] =
      execute(kpc.endOffsets.flatMap(x => kpc.commitSync(offsetsOf(x))))

    override def resetOffsetsForTimes(ts: Instant): F[Unit] =
      execute(kpc.offsetsForTimes(ts).flatMap(x => kpc.commitSync(offsetsOf(x))))

    override val metrics: F[Map[MetricName, Metric]] =
      execute(kpc.metrics)

  }
}
