package com.github.chenharryhua.nanjin.kafka.connector

import cats.data.ReaderT
import cats.effect.Concurrent
import com.github.chenharryhua.nanjin.kafka.{OffsetRange, PartitionRange, TopicPartitionMap}
import fs2.Stream
import fs2.kafka.CommittableConsumerRecord
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

trait RangedStream[F[_], K, V] {
  def stopConsuming: F[Unit]

  def partitionsMapStream: Map[PartitionRange, Stream[F, CommittableConsumerRecord[F, K, V]]]

  final def stream(implicit F: Concurrent[F]): Stream[F, CommittableConsumerRecord[F, K, V]] =
    Stream.iterable(partitionsMapStream.values).parJoinUnbounded.onFinalize(stopConsuming)

  final def offsets: TopicPartitionMap[OffsetRange] =
    TopicPartitionMap(partitionsMapStream.keySet.map(pr => pr.topicPartition -> pr.offsetRange))
}

trait ManualCommitStream[F[_], K, V] {
  def commitSync: ReaderT[F, Map[TopicPartition, OffsetAndMetadata], Unit]
  def commitAsync: ReaderT[F, Map[TopicPartition, OffsetAndMetadata], Unit]

  def partitionsMapStream: Map[TopicPartition, Stream[F, CommittableConsumerRecord[F, K, V]]]

  final def stream(implicit F: Concurrent[F]): Stream[F, CommittableConsumerRecord[F, K, V]] =
    Stream.iterable(partitionsMapStream.values).parJoinUnbounded
}
