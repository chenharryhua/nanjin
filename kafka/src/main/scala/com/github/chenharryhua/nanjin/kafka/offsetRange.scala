package com.github.chenharryhua.nanjin.kafka

import cats.syntax.all.*

object offsetRange {

  def apply(
    start: KafkaTopicPartition[Option[KafkaOffset]],
    end: KafkaTopicPartition[Option[KafkaOffset]]): KafkaTopicPartition[Option[KafkaOffsetRange]] =
    start.intersectCombine(end)(Tuple2(_, _).mapN(KafkaOffsetRange(_, _)).flatten)

  def apply(
    start: KafkaTopicPartition[Option[KafkaOffset]],
    end: KafkaTopicPartition[Option[KafkaOffset]],
    to: Option[KafkaTopicPartition[Option[KafkaOffset]]]): KafkaTopicPartition[Option[KafkaOffsetRange]] = {
    val endOffset = to.fold(end)(_.intersectCombine(end)(_.orElse(_)))
    apply(start, endOffset)
  }
}
