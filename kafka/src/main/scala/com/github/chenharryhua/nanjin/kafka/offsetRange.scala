package com.github.chenharryhua.nanjin.kafka

import cats.syntax.all.*

object offsetRange {

  def apply(
    start: KafkaTopicPartition[Option[KafkaOffset]],
    end: KafkaTopicPartition[Option[KafkaOffset]]): KafkaTopicPartition[Option[KafkaOffsetRange]] =
    start.combineWith(end)(Tuple2(_, _).mapN(KafkaOffsetRange(_, _)).flatten)

  def apply(
    start: KafkaTopicPartition[Option[KafkaOffset]],
    end: KafkaTopicPartition[Option[KafkaOffset]],
    to: Option[KafkaTopicPartition[Option[KafkaOffset]]]): KafkaTopicPartition[Option[KafkaOffsetRange]] = {
    val endOffset = to.fold(end)(_.combineWith(end)(_.orElse(_)))
    apply(start, endOffset)
  }
}
