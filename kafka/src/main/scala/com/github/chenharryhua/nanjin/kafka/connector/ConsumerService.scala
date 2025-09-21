package com.github.chenharryhua.nanjin.kafka.connector

import cats.effect.kernel.Resource
import com.github.chenharryhua.nanjin.datetime.DateTimeRange
import fs2.Stream
import fs2.kafka.{CommittableConsumerRecord, KafkaConsumer}

import java.time.Instant

abstract class ConsumerService[F[_], K, V] {
  def clientR: Resource[F, KafkaConsumer[F, K, V]]
  def clientS: Stream[F, KafkaConsumer[F, K, V]]

  def subscribe: Stream[F, CommittableConsumerRecord[F, K, V]]

  def assign: Stream[F, CommittableConsumerRecord[F, K, V]]
  def assign(pos: Map[Int, Long]): Stream[F, CommittableConsumerRecord[F, K, V]]
  def assign(time: Instant): Stream[F, CommittableConsumerRecord[F, K, V]]

  def manualCommitStream: Stream[F, ManualCommitStream[F, K, V]]

  def circumscribedStream(dateTimeRange: DateTimeRange): Stream[F, CircumscribedStream[F, K, V]]
  def circumscribedStream(pos: Map[Int, (Long, Long)]): Stream[F, CircumscribedStream[F, K, V]]
}
