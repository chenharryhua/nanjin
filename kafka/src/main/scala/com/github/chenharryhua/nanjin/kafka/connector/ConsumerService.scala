package com.github.chenharryhua.nanjin.kafka.connector

import com.github.chenharryhua.nanjin.datetime.DateTimeRange
import fs2.Stream
import fs2.kafka.CommittableConsumerRecord

import java.time.Instant

trait ConsumerService[F[_], K, V] {
  def subscribe: Stream[F, CommittableConsumerRecord[F, K, V]]

  def assign: Stream[F, CommittableConsumerRecord[F, K, V]]
  def assign(partitionOffsets: Map[Int, Long]): Stream[F, CommittableConsumerRecord[F, K, V]]
  def assign(time: Instant): Stream[F, CommittableConsumerRecord[F, K, V]]

  def manualCommitStream: Stream[F, ManualCommitStream[F, K, V]]

  def circumscribedStream(dateTimeRange: DateTimeRange): Stream[F, CircumscribedStream[F, K, V]]
  def circumscribedStream(partitionOffsets: Map[Int, (Long, Long)]): Stream[F, CircumscribedStream[F, K, V]]
}
