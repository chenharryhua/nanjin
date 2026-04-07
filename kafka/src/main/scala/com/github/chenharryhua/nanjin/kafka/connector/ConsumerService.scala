package com.github.chenharryhua.nanjin.kafka.connector

import cats.data.NonEmptyList
import cats.effect.kernel.Sync
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import cats.syntax.apply.given 
import cats.syntax.show.showInterpolator
import cats.syntax.traverse.given
import com.github.chenharryhua.nanjin.datetime.DateTimeRange
import com.github.chenharryhua.nanjin.kafka.{TopicName, TopicPartitionMap}
import fs2.Stream
import fs2.kafka.CommittableConsumerRecord
import fs2.kafka.consumer.{KafkaAssignment, KafkaOffsets, KafkaTopicsV2}
import org.apache.kafka.common.TopicPartition

import java.time.Instant

/*
 * Consumer Service
 */
trait ConsumerService[F[_], K, V] {
  protected def assignByTime(
    kc: KafkaAssignment[F] & KafkaTopicsV2[F] & KafkaOffsets[F],
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

  protected def assignByMap(
    kc: KafkaAssignment[F] & KafkaTopicsV2[F] & KafkaOffsets[F],
    tn: TopicName,
    map: Map[Int, Long])(using F: Sync[F]): F[Unit] = {
    val tpm = TopicPartitionMap(map.map { case (p, o) => new TopicPartition(tn.value, p) -> o })

    tpm.nonEmptyKeySet match {
      case Some(value) => kc.assign(value) <* tpm.toList.traverse { case (p, o) => kc.seek(p, o) }
      case None        => F.raiseError(new Exception(show"empty map of $tn"))
    }
  }

  def subscribe: Stream[F, CommittableConsumerRecord[F, K, V]]

  def partitionsMapStream: Stream[F, TopicPartitionMap[Stream[F, CommittableConsumerRecord[F, K, V]]]]

  def assign: Stream[F, CommittableConsumerRecord[F, K, V]]
  def assign(partitionOffsets: Map[Int, Long]): Stream[F, CommittableConsumerRecord[F, K, V]]
  def assign(time: Instant): Stream[F, CommittableConsumerRecord[F, K, V]]

  def manualCommitStream: Stream[F, ManualCommitStream[F, K, V]]

  def circumscribedStream(dateTimeRange: DateTimeRange): Stream[F, CircumscribedStream[F, K, V]]
  def circumscribedStream(partitionOffsets: Map[Int, (Long, Long)]): Stream[F, CircumscribedStream[F, K, V]]
}
