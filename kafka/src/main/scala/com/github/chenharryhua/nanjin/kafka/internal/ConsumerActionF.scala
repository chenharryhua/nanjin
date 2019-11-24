package com.github.chenharryhua.nanjin.kafka.internal

import cats.Functor
import com.github.chenharryhua.nanjin.datetime.NJTimestamp
import com.github.chenharryhua.nanjin.kafka.{GenericTopicPartition, KafkaOffset}
import fs2.kafka.KafkaByteConsumer
import org.apache.kafka.common.PartitionInfo

sealed trait ConsumerActionF[A]

object ConsumerActionF {
  import cats.implicits._

  final case class PartitionsFor[A](kont: GenericTopicPartition[PartitionInfo] => A)
      extends ConsumerActionF[A]

  final case class BeginningOffsets[A](kont: GenericTopicPartition[Option[KafkaOffset]] => A)
      extends ConsumerActionF[A]

  final case class EndOffsets[A](kont: GenericTopicPartition[Option[KafkaOffset]] => A)
      extends ConsumerActionF[A]

  final case class OffsetsForTimes[A](
    ts: NJTimestamp,
    kont: GenericTopicPartition[Option[KafkaOffset]] => A)
      extends ConsumerActionF[A]

  implicit val functorConsumerAction: Functor[ConsumerActionF] =
    cats.derived.semi.functor[ConsumerActionF]
}
