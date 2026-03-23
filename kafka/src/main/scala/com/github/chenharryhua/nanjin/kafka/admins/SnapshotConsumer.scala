package com.github.chenharryhua.nanjin.kafka.admins

import cats.Monad
import cats.data.Kleisli
import cats.effect.kernel.{Resource, Sync}
import cats.mtl.Ask
import cats.syntax.flatMap.toFlatMapOps
import cats.syntax.functor.toFunctorOps
import cats.syntax.functorFilter.toFunctorFilterOps
import cats.syntax.traverse.toTraverseOps
import com.github.chenharryhua.nanjin.datetime.{DateTimeRange, NJTimestamp}
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

import java.time.Duration
import scala.jdk.CollectionConverters.*

sealed trait KafkaConsumerOps[F[_]] {
  def partitionsFor: F[TopicPartitionList]
  def beginningOffsets: F[TopicPartitionMap[Option[Offset]]]
  def endOffsets: F[TopicPartitionMap[Option[Offset]]]
  def offsetsForTimes(ts: NJTimestamp): F[TopicPartitionMap[Option[Offset]]]

  def retrieveRecord(
    partition: Partition,
    offset: Offset): F[Option[ConsumerRecord[Array[Byte], Array[Byte]]]]

  def commitSync(offsets: Map[TopicPartition, OffsetAndMetadata]): F[Unit]

  def metrics: F[Map[MetricName, Metric]]
}

private object KafkaConsumerOps {

  def apply[F[_]: Monad](topicName: TopicName)(using F: Ask[F, KafkaByteConsumer]): KafkaConsumerOps[F] =
    new KafkaPrimitiveConsumerApiImpl[F](topicName)

  final private class KafkaPrimitiveConsumerApiImpl[F[_]: Monad](topicName: TopicName)(using
    kbc: Ask[F, KafkaByteConsumer]
  ) extends KafkaConsumerOps[F] {

    override val partitionsFor: F[TopicPartitionList] =
      kbc.ask.map { c =>
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
        ret <- kbc.ask.map(_.beginningOffsets(tps.javaList).asScala)
      } yield TopicPartitionMap(ret.map { case (k, v) => k -> Option(v).map(Offset(_)) })

    override val endOffsets: F[TopicPartitionMap[Option[Offset]]] =
      for {
        tps <- partitionsFor
        ret <- kbc.ask.map(_.endOffsets(tps.javaList).asScala)
      } yield TopicPartitionMap(ret.map { case (k, v) => k -> Option(v).map(Offset(_)) })

    override def offsetsForTimes(ts: NJTimestamp): F[TopicPartitionMap[Option[Offset]]] =
      for {
        tps <- partitionsFor
        ret <- kbc.ask.map(_.offsetsForTimes(tps.javaTimed(ts)).asScala)
      } yield TopicPartitionMap(ret.map { case (k, v) => k -> Option(v).map(Offset(_)) })

    override def retrieveRecord(
      partition: Partition,
      offset: Offset): F[Option[ConsumerRecord[Array[Byte], Array[Byte]]]] =
      kbc.ask.map { consumer =>
        val tp = new TopicPartition(topicName.value, partition.value)
        consumer.assign(List(tp).asJava)
        consumer.seek(tp, offset.value)
        consumer.poll(Duration.ofSeconds(15)).records(tp).asScala.toList.headOption
      }

    override def commitSync(offsets: Map[TopicPartition, OffsetAndMetadata]): F[Unit] =
      kbc.ask.map(_.commitSync(offsets.asJava))

    override val metrics: F[Map[MetricName, Metric]] =
      kbc.ask.map(_.metrics().asScala.toMap)
  }
}

sealed trait SnapshotConsumer[F[_]] extends KafkaConsumerOps[F] {
  def offsetRangeFor(dtr: DateTimeRange): F[TopicPartitionMap[Option[OffsetRange]]]
  def offsetRangeFor(start: NJTimestamp, end: NJTimestamp): F[TopicPartitionMap[Option[OffsetRange]]]
  def offsetRangeForAll: F[TopicPartitionMap[Option[OffsetRange]]]

  def retrieveLastRecords: F[List[ConsumerRecord[Array[Byte], Array[Byte]]]]
  def retrieveFirstRecords: F[List[ConsumerRecord[Array[Byte], Array[Byte]]]]
  def retrieveRecordsForTimes(ts: NJTimestamp): F[List[ConsumerRecord[Array[Byte], Array[Byte]]]]
  def numOfRecordsSince(ts: NJTimestamp): F[TopicPartitionMap[Option[OffsetRange]]]

  def resetOffsetsToBegin: F[Unit]
  def resetOffsetsToEnd: F[Unit]
  def resetOffsetsForTimes(ts: NJTimestamp): F[Unit]
}

private[kafka] object SnapshotConsumer {

  def apply[F[_]: Sync](topicName: TopicName, cs: PureConsumerSettings): Resource[F, SnapshotConsumer[F]] =
    makePureConsumer(cs).map(consumer => new SnapshotConsumerImpl(topicName, consumer))
  def apply[F[_]: Sync](topicName: TopicName, consumer: KafkaByteConsumer): SnapshotConsumer[F] =
    new SnapshotConsumerImpl[F](topicName, consumer)

  final private class SnapshotConsumerImpl[F[_]: Sync](topicName: TopicName, consumer: KafkaByteConsumer)
      extends SnapshotConsumer[F] {
    def withTopicName(name: TopicName): SnapshotConsumerImpl[F] =
      new SnapshotConsumerImpl[F](name, consumer)

    private val kpc: KafkaConsumerOps[Kleisli[F, KafkaByteConsumer, *]] =
      KafkaConsumerOps[Kleisli[F, KafkaByteConsumer, *]](topicName)

    private def execute[A](r: Kleisli[F, KafkaByteConsumer, A]): F[A] =
      r.run(consumer)

    override def offsetRangeFor(dtr: DateTimeRange): F[TopicPartitionMap[Option[OffsetRange]]] =
      execute {
        for {
          from <- dtr.startTimestamp.fold(kpc.beginningOffsets)(kpc.offsetsForTimes)
          end <- kpc.endOffsets
          to <- dtr.endTimestamp.traverse(kpc.offsetsForTimes)
        } yield calculate.consumer_offsetRange(from, end, to)
      }

    override def offsetRangeFor(
      start: NJTimestamp,
      end: NJTimestamp): F[TopicPartitionMap[Option[OffsetRange]]] =
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

    override def retrieveRecordsForTimes(ts: NJTimestamp): F[List[ConsumerRecord[Array[Byte], Array[Byte]]]] =
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

    override def numOfRecordsSince(ts: NJTimestamp): F[TopicPartitionMap[Option[OffsetRange]]] =
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

    override def offsetsForTimes(ts: NJTimestamp): F[TopicPartitionMap[Option[Offset]]] =
      execute(kpc.offsetsForTimes(ts))

    override def retrieveRecord(
      partition: Partition,
      offset: Offset): F[Option[ConsumerRecord[Array[Byte], Array[Byte]]]] =
      execute(kpc.retrieveRecord(partition, offset))

    override def commitSync(offsets: Map[TopicPartition, OffsetAndMetadata]): F[Unit] =
      execute(kpc.commitSync(offsets))

    private def offsetsOf(
      offsets: TopicPartitionMap[Option[Offset]]): Map[TopicPartition, OffsetAndMetadata] =
      offsets.flatten.treeMap.map { case (k, offset) => k -> new OffsetAndMetadata(offset.value) }

    override val resetOffsetsToBegin: F[Unit] =
      execute(kpc.beginningOffsets.flatMap(x => kpc.commitSync(offsetsOf(x))))

    override val resetOffsetsToEnd: F[Unit] =
      execute(kpc.endOffsets.flatMap(x => kpc.commitSync(offsetsOf(x))))

    override def resetOffsetsForTimes(ts: NJTimestamp): F[Unit] =
      execute(kpc.offsetsForTimes(ts).flatMap(x => kpc.commitSync(offsetsOf(x))))

    override val metrics: F[Map[MetricName, Metric]] =
      execute(kpc.metrics)

  }
}
