package com.github.chenharryhua.nanjin.kafka.connector

import cats.Monad
import cats.data.NonEmptySet
import cats.effect.kernel.Sync
import cats.implicits.*
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.datetime.DateTimeRange
import com.github.chenharryhua.nanjin.kafka.{
  Offset,
  OffsetRange,
  PartitionRange,
  PullGenericRecord,
  TopicPartitionMap
}
import com.github.chenharryhua.nanjin.messages.kafka.CRMetaInfo
import fs2.Stream
import fs2.kafka.consumer.{KafkaConsume, KafkaTopicsV2}
import fs2.kafka.{CommittableConsumerRecord, KafkaConsumer}
import org.apache.avro.generic.GenericData
import org.apache.kafka.common.TopicPartition

import scala.util.{Failure, Success}

private object consumerClient {
  def get_offset_range[F[_]: Monad](
    client: KafkaTopicsV2[F],
    topicName: TopicName,
    dtr: DateTimeRange): F[TopicPartitionMap[OffsetRange]] =
    client.partitionsFor(topicName.value).flatMap { pis =>
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

  def assign_range[F[_]: Monad, K, V](
    client: KafkaConsumer[F, K, V],
    ranges: TopicPartitionMap[OffsetRange]): F[Unit] =
    NonEmptySet
      .fromSet(ranges.value.keySet)
      .traverse(tps =>
        client.assign(tps) *> tps.toNonEmptyList.traverse(tp =>
          ranges.get(tp).traverse(or => client.seek(tp, or.from))))
      .void

  def ranged_stream[F[_], K, V](
    client: KafkaConsume[F, K, V],
    ranges: TopicPartitionMap[OffsetRange]): Stream[F, RangedStream[F, CommittableConsumerRecord[F, K, V]]] =
    client.partitionsMapStream.map { pms =>
      val streams: Map[PartitionRange, Stream[F, CommittableConsumerRecord[F, K, V]]] =
        pms.toList.mapFilter { case (tp, stream) =>
          ranges.get(tp).map { offsetRange =>
            PartitionRange(tp, offsetRange) ->
              stream.takeWhile(_.record.offset < offsetRange.to, takeFailure = true)
          }
        }.toMap
      RangedStream(client.stopConsuming, streams)
    }

  def ranged_gr_stream[F[_]](
    client: KafkaConsume[F, Array[Byte], Array[Byte]],
    ranges: TopicPartitionMap[OffsetRange],
    pull: PullGenericRecord,
    ignoreError: Boolean)(implicit F: Sync[F]): Stream[F, RangedStream[F, GenericData.Record]] =
    client.partitionsMapStream.map { pms =>
      val streams: Map[PartitionRange, Stream[F, GenericData.Record]] =
        pms.toList.mapFilter { case (tp, stream) =>
          ranges.get(tp).map { offsetRange =>
            val sgr: Stream[F, GenericData.Record] =
              if (ignoreError) {
                stream.takeWhile(_.record.offset < offsetRange.to, takeFailure = true).mapChunks {
                  _.mapFilter(ccr => pull.toGenericRecord(ccr.record).toOption)
                }
              } else {
                stream.takeWhile(_.record.offset < offsetRange.to, takeFailure = true).evalMapChunk { ccr =>
                  pull.toGenericRecord(ccr.record) match {
                    case Success(value)     => F.pure(value)
                    case Failure(exception) =>
                      F.raiseError[GenericData.Record](PullDecodeException(CRMetaInfo(ccr.record), exception))
                  }
                }
              }

            PartitionRange(tp, offsetRange) -> sgr
          }
        }.toMap
      RangedStream(client.stopConsuming, streams)
    }
}
