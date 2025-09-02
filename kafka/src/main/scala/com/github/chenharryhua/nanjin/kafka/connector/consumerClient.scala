package com.github.chenharryhua.nanjin.kafka.connector

import cats.Monad
import cats.data.NonEmptySet
import cats.effect.kernel.Sync
import cats.implicits.*
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.datetime.DateTimeRange
import com.github.chenharryhua.nanjin.kafka.{
  calculate,
  Offset,
  OffsetRange,
  PartitionRange,
  PullGenericRecord,
  TopicPartitionMap
}
import fs2.Stream
import fs2.kafka.consumer.{KafkaConsume, KafkaTopicsV2}
import fs2.kafka.{CommittableConsumerRecord, KafkaConsumer}
import org.apache.avro.generic.GenericData
import org.apache.kafka.common.TopicPartition

private object consumerClient {
  def get_offset_range[F[_]: Monad](
    client: KafkaTopicsV2[F],
    topicName: TopicName,
    dtr: DateTimeRange): F[TopicPartitionMap[Option[OffsetRange]]] =
    for {
      partitions <- client.partitionsFor(topicName.value)
      tps = partitions.map(pi => new TopicPartition(pi.topic(), pi.partition()))
      from <- dtr.startTimestamp.fold(
        client.beginningOffsets(tps.toSet).map(TopicPartitionMap(_).mapValues(o => Offset(o).some))) { ts =>
        client
          .offsetsForTimes(tps.map(tp => tp -> ts.milliseconds).toMap)
          .map(TopicPartitionMap(_).mapValues(_.map(o => Offset(o.offset()))))
      }
      end <- client.endOffsets(tps.toSet).map(TopicPartitionMap(_).mapValues(o => Offset(o).some))
      to <- dtr.endTimestamp
        .traverse(ts => client.offsetsForTimes(tps.map(tp => tp -> ts.milliseconds).toMap))
        .map(_.map(TopicPartitionMap(_).mapValues(_.map(o => Offset(o.offset())))))
    } yield calculate.consumer_offsetRange(from, end, to)

  def assign_range[F[_]: Monad, K, V](
    client: KafkaConsumer[F, K, V],
    tpm: TopicPartitionMap[OffsetRange]): F[Unit] =
    NonEmptySet
      .fromSet(tpm.value.keySet)
      .traverse(tps =>
        client.assign(tps) *> tps.toNonEmptyList.traverse(tp =>
          tpm.get(tp).traverse(or => client.seek(tp, or.from))))
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
    pull: PullGenericRecord)(implicit F: Sync[F]): Stream[F, RangedStream[F, GenericData.Record]] =
    client.partitionsMapStream.map { pms =>
      val streams: Map[PartitionRange, Stream[F, GenericData.Record]] =
        pms.toList.mapFilter { case (tp, stream) =>
          ranges.get(tp).map { offsetRange =>
            PartitionRange(tp, offsetRange) ->
              stream.takeWhile(_.record.offset < offsetRange.to, takeFailure = true).evalMapChunk { ccr =>
                F.fromTry(pull.toGenericRecord(ccr.record))
              }
          }
        }.toMap
      RangedStream(client.stopConsuming, streams)
    }
}
