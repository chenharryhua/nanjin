package com.github.chenharryhua.nanjin.kafka.internal

import cats.Functor
import com.github.chenharryhua.nanjin.kafka.GenericTopicPartition
import fs2.kafka.KafkaByteConsumer
import org.apache.kafka.common.PartitionInfo

sealed trait ConsumerActionF[A]

object ConsumerActionF {
  import cats.implicits._

  final case class ObtainKafkaByteConsumer[A](kont: KafkaByteConsumer => A)
      extends ConsumerActionF[A]

  final case class PartitionsFor[A](kont: GenericTopicPartition[PartitionInfo] => A)
      extends ConsumerActionF[A]
  final case class BeginningOffsets[A]() extends ConsumerActionF[A]
  final case class EndOffsets[A]() extends ConsumerActionF[A]
  final case class OffsetsForTimes[A]() extends ConsumerActionF[A]

  implicit val functorConsumerAction: Functor[ConsumerActionF] =
    cats.derived.semi.functor[ConsumerActionF]
}
