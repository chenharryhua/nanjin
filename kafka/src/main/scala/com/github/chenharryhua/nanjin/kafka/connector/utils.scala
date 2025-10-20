package com.github.chenharryhua.nanjin.kafka.connector

import cats.data.NonEmptySet
import cats.implicits.*
import cats.{Applicative, Monad}
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.datetime.DateTimeRange
import com.github.chenharryhua.nanjin.kafka.{Offset, OffsetRange, PartitionRange, TopicPartitionMap}
import fs2.Stream
import fs2.kafka.consumer.{KafkaConsume, KafkaTopicsV2}
import fs2.kafka.{CommittableConsumerRecord, KafkaConsumer}
import org.apache.avro.generic.GenericData
import org.apache.kafka.common.TopicPartition

private object utils {
  private def get_offset_range_by_time[F[_]: Monad](
    client: KafkaTopicsV2[F],
    topicName: TopicName,
    dtr: DateTimeRange): F[TopicPartitionMap[OffsetRange]] =
    client.partitionsFor(topicName.name.value).flatMap { pis =>
      val tps = pis.map(pi => new TopicPartition(pi.topic(), pi.partition()))

      val start_offsets: F[TopicPartitionMap[Long]] = {
        val start_time = dtr.startTimestamp.map(_.milliseconds).getOrElse(0L)
        client
          .offsetsForTimes(tps.map(_ -> start_time).toMap)
          .map(TopicPartitionMap(_).flatten.mapValues(_.offset()))
      }

      val end_offsets: F[TopicPartitionMap[Long]] =
        client.endOffsets(tps.toSet).map(TopicPartitionMap(_)).flatMap { topic_end =>
          dtr.endTimestamp.map(_.milliseconds) match {
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

  private def get_offset_range_by_offsets[F[_]: Monad](
    client: KafkaTopicsV2[F],
    topicName: TopicName,
    pos: Map[Int, (Long, Long)]): F[TopicPartitionMap[OffsetRange]] =
    for {
      pis <- client.partitionsFor(topicName.name.value)
      tps = pis.map(pi => new TopicPartition(pi.topic(), pi.partition())).toSet
      topic_begin <- client.beginningOffsets(tps).map(TopicPartitionMap(_))
      topic_end <- client.endOffsets(tps).map(TopicPartitionMap(_))
    } yield {
      val origin: TopicPartitionMap[OffsetRange] =
        TopicPartitionMap(pos.map { case (partition, (from, until)) =>
          new TopicPartition(topicName.name.value, partition) -> OffsetRange(Offset(from), Offset(until))
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

  def get_offset_range[F[_]: Monad](
    client: KafkaTopicsV2[F],
    topicName: TopicName,
    or: Either[DateTimeRange, Map[Int, (Long, Long)]]): F[TopicPartitionMap[OffsetRange]] =
    or match {
      case Left(value)  => get_offset_range_by_time(client, topicName, value)
      case Right(value) => get_offset_range_by_offsets(client, topicName, value)
    }

  def assign_offset_range[F[_]: Applicative, K, V](
    client: KafkaConsumer[F, K, V],
    ranges: TopicPartitionMap[OffsetRange]): F[Boolean] =
    NonEmptySet
      .fromSet(ranges.value.keySet)
      .traverse(tps =>
        client.assign(tps) *> tps.toNonEmptyList.toList.traverse(tp =>
          ranges.get(tp).traverse(or => client.seek(tp, or.from))))
      .map(_.traverse(_.flatten).flatten.nonEmpty)

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
        override def partitionsMapStream: Map[PartitionRange, Stream[F, CommittableConsumerRecord[F, K, V]]] =
          streams
      }
    }

  def circumscribed_generic_record_stream[F[_]](
    client: KafkaConsume[F, Array[Byte], Array[Byte]],
    ranges: TopicPartitionMap[OffsetRange],
    pull: PullGenericRecord)
    : Stream[F, CircumscribedStream[F, Unit, Either[PullGenericRecordException, GenericData.Record]]] =
    client.partitionsMapStream.map { pms =>
      val streams: Map[
        PartitionRange,
        Stream[
          F,
          CommittableConsumerRecord[F, Unit, Either[PullGenericRecordException, GenericData.Record]]]] =
        pms.toList.mapFilter { case (tp, stream) =>
          ranges.get(tp).map { offsetRange =>
            val sgr: Stream[
              F,
              CommittableConsumerRecord[F, Unit, Either[PullGenericRecordException, GenericData.Record]]] =
              stream.takeWhile(_.record.offset < offsetRange.to, takeFailure = true).mapChunks { crs =>
                crs.map(cr => cr.bimap(_ => (), _ => pull.toGenericRecord(cr.record)))
              }

            PartitionRange(tp, offsetRange) -> sgr
          }
        }.toMap

      new CircumscribedStream[F, Unit, Either[PullGenericRecordException, GenericData.Record]] {
        override def stopConsuming: F[Unit] =
          client.stopConsuming
        override def partitionsMapStream: Map[
          PartitionRange,
          Stream[
            F,
            CommittableConsumerRecord[F, Unit, Either[PullGenericRecordException, GenericData.Record]]]] =
          streams
      }
    }
}
