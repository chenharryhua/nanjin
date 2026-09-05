package com.github.chenharryhua.nanjin.kafka.connector

import cats.syntax.applicative.given
import cats.syntax.apply.given
import cats.syntax.bifunctor.given
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import cats.syntax.functorFilter.given
import cats.syntax.traverse.given
import cats.{Applicative, Monad}
import com.github.chenharryhua.nanjin.datetime.DateTimeRange
import com.github.chenharryhua.nanjin.kafka.{
  Offset,
  OffsetRange,
  PartitionRange,
  TopicName,
  TopicPartitionMap
}
import fs2.Stream
import fs2.kafka.consumer.{KafkaConsume, KafkaTopics}
import fs2.kafka.{CommittableConsumerRecord, KafkaConsumer}
import org.apache.avro.generic.GenericData.Record
import org.apache.kafka.common.TopicPartition

/** Helpers for resolving and consuming bounded offset ranges, backing the `circumscribedStream` modes of the
  * consumer connectors.
  *
  * The `get_offset_range_*` functions turn a request (a `DateTimeRange` or explicit per-partition offset
  * bounds) into a concrete `TopicPartitionMap[OffsetRange]`, always clamped to the partitions' actual
  * `[beginning, end)` extents; partitions with an empty or invalid range are dropped. `assign_offset_range`
  * assigns those partitions and seeks each to its start, and the `circumscribed_*_stream` functions build
  * bounded streams that stop each partition once it passes its range end.
  */
private object topicUtils {

  /** Resolve offset ranges from a wall-clock `DateTimeRange`: map the range's start/end to per-partition
    * offsets via `offsetsForTimes`, defaulting the start to 0 and the end to each partition's end offset when
    * unbounded.
    */
  private def get_offset_range_by_time[F[_]: Monad](
    client: KafkaTopics[F],
    topicName: TopicName,
    dtr: DateTimeRange): F[TopicPartitionMap[OffsetRange]] =
    client.partitionsFor(topicName.value).flatMap { pis =>
      val tps = pis.map(pi => new TopicPartition(pi.topic(), pi.partition()))

      val start_offsets: F[TopicPartitionMap[Long]] = {
        val start_time = dtr.start.map(_.toEpochMilli).getOrElse(0L)
        client
          .offsetsForTimes(tps.map(_ -> start_time).toMap)
          .map(TopicPartitionMap(_).flatten.mapValues(_.offset()))
      }

      val end_offsets: F[TopicPartitionMap[Long]] =
        client.endOffsets(tps.toSet).map(TopicPartitionMap(_)).flatMap { topic_end =>
          dtr.end.map(_.toEpochMilli) match {
            case Some(end_time) =>
              client.offsetsForTimes(tps.map(_ -> end_time).toMap).map {
                TopicPartitionMap(_).intersectCombine(topic_end) {
                  _.map(_.offset()).getOrElse(_)
                }
              }
            case _ => Monad[F].pure(topic_end)
          }
        }

      (start_offsets, end_offsets).mapN {
        _.intersectCombine(_)((s, e) => OffsetRange(Offset(s), Offset(e))).flatten
      }
    }

  /** Resolve offset ranges from explicit per-partition `(from, until)` bounds, intersected with each
    * partition's actual `[beginning, end)`: the effective start is `max(topicBegin, from)` and end is
    * `min(topicEnd, until)`. Partitions not present on the topic, or whose clamped range is empty, are
    * dropped.
    */
  private def get_offset_range_by_offsets[F[_]: Monad](
    client: KafkaTopics[F],
    topicName: TopicName,
    pos: Map[Int, (Long, Long)]): F[TopicPartitionMap[OffsetRange]] =
    for {
      pis <- client.partitionsFor(topicName.value)
      tps = pis.map(pi => new TopicPartition(pi.topic(), pi.partition())).toSet
      topic_begin <- client.beginningOffsets(tps).map(TopicPartitionMap(_))
      topic_end <- client.endOffsets(tps).map(TopicPartitionMap(_))
    } yield {
      val origin: TopicPartitionMap[OffsetRange] =
        TopicPartitionMap(pos.map { case (partition, (from, until)) =>
          new TopicPartition(topicName.value, partition) -> OffsetRange(Offset(from), Offset(until))
        }).flatten

      val topic_range: TopicPartitionMap[OffsetRange] =
        topic_begin.intersectCombine(topic_end)((s, e) => OffsetRange(Offset(s), Offset(e))).flatten

      topic_range
        .intersectCombine(origin) { (tr, o) =>
          val start = Math.max(tr.from, o.from)
          val end = Math.min(tr.until, o.until)
          OffsetRange(Offset(start), Offset(end))
        }
        .flatten
    }

  /** Resolve offset ranges from either a `DateTimeRange` (`Left`) or explicit per-partition bounds (`Right`),
    * dispatching to the matching helper.
    */
  def get_offset_range[F[_]: Monad](
    client: KafkaTopics[F],
    topicName: TopicName,
    or: Either[DateTimeRange, Map[Int, (Long, Long)]]): F[TopicPartitionMap[OffsetRange]] =
    or match {
      case Left(value)  => get_offset_range_by_time(client, topicName, value)
      case Right(value) => get_offset_range_by_offsets(client, topicName, value)
    }

  /** Assign the consumer to the ranges' partitions and seek each to its `from` offset. Returns `true` if any
    * partitions were assigned, `false` if the range map was empty (nothing to consume).
    */
  def assign_offset_range[F[_]: Applicative, K, V](
    client: KafkaConsumer[F, K, V],
    ranges: TopicPartitionMap[OffsetRange]): F[Boolean] =
    ranges.nonEmptyKeySet match {
      case Some(tps) =>
        client.assign(tps) *> ranges.toList.traverse((tp, or) => client.seek(tp, or.from)).map(_.nonEmpty)
      case None => false.pure[F]
    }

  /** Build a bounded `CircumscribedStream` of typed records: for each partition with a resolved range, take
    * records until the offset reaches the range end (`takeFailure = true` keeps the boundary record), keyed
    * by `PartitionRange`.
    */
  def circumscribed_stream[F[_], K, V](
    client: KafkaConsume[F, K, V],
    ranges: TopicPartitionMap[OffsetRange]): Stream[F, CircumscribedStream[F, K, V]] =
    client.partitionsMapStream.map { pms =>
      val streams: Map[PartitionRange, Stream[F, CommittableConsumerRecord[F, K, V]]] =
        pms.toList.mapFilter { case (tp, stream) =>
          ranges.get(tp).map { offsetRange =>
            PartitionRange(tp, offsetRange) ->
              stream.takeWhile(_.record.offset < offsetRange.to, takeFailure = true)
          }
        }.toMap

      new CircumscribedStream[F, K, V] {
        override def stopConsuming: F[Unit] =
          client.stopConsuming
        override def rangedStreams: Map[PartitionRange, Stream[F, CommittableConsumerRecord[F, K, V]]] =
          streams
      }
    }

  /** Like `circumscribed_stream`, but decodes each raw byte record to `Either[PullError, Record]` via `pull`,
    * for the generic-record consumer. Same range-bounding behavior per partition.
    */
  def circumscribed_generic_record_stream[F[_]](
    client: KafkaConsume[F, Array[Byte], Array[Byte]],
    ranges: TopicPartitionMap[OffsetRange],
    pull: PullGenericRecord): Stream[F, CircumscribedStream[F, Unit, Either[PullError, Record]]] =
    client.partitionsMapStream.map { pms =>
      val streams
        : Map[PartitionRange, Stream[F, CommittableConsumerRecord[F, Unit, Either[PullError, Record]]]] =
        pms.toList.mapFilter { case (tp, stream) =>
          ranges.get(tp).map { offsetRange =>
            val sgr: Stream[F, CommittableConsumerRecord[F, Unit, Either[PullError, Record]]] =
              stream.takeWhile(_.record.offset < offsetRange.to, takeFailure = true)
                .mapChunks { crs =>
                  crs.map(cr => cr.bimap(_ => (), _ => pull.toGenericRecord(cr.record)))
                }

            PartitionRange(tp, offsetRange) -> sgr
          }
        }.toMap

      new CircumscribedStream[F, Unit, Either[PullError, Record]] {
        override def stopConsuming: F[Unit] =
          client.stopConsuming
        override def rangedStreams
          : Map[PartitionRange, Stream[F, CommittableConsumerRecord[F, Unit, Either[PullError, Record]]]] =
          streams
      }
    }
}
