package com.github.chenharryhua.nanjin.kafka.connector

import fs2.Stream
import fs2.kafka.CommittableConsumerRecord
import fs2.kafka.consumer.{KafkaCommit, KafkaConsume}
import org.apache.kafka.common.TopicPartition

final case class ManualCommitStream[F[_], K, V](
  committer: KafkaCommit[F],
  stream: Stream[F, CommittableConsumerRecord[F, K, V]])

final case class ManualStoppableStream[F[_], K, V](
  kafkaConsume: KafkaConsume[F, K, V],
  streams: Stream[F, Map[TopicPartition, Stream[F, CommittableConsumerRecord[F, K, V]]]]
)
