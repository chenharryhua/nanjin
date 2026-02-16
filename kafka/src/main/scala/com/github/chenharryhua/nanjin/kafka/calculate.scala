package com.github.chenharryhua.nanjin.kafka

import cats.syntax.apply.catsSyntaxTuple2Semigroupal

private object calculate {
  def admin_lagBehind(
    ends: TopicPartitionMap[Option[Offset]],
    curr: TopicPartitionMap[Offset]): TopicPartitionMap[Option[LagBehind]] =
    ends.leftCombine(curr)((e, c) => e.map(LagBehind(c, _)))

  def consumer_offsetRange(
    start: TopicPartitionMap[Option[Offset]],
    end: TopicPartitionMap[Option[Offset]]): TopicPartitionMap[Option[OffsetRange]] =
    start.leftCombine(end) { case (k, v) => (k, v).flatMapN(OffsetRange(_, _)) }

  def consumer_offsetRange(
    start: TopicPartitionMap[Option[Offset]],
    end: TopicPartitionMap[Option[Offset]],
    to: Option[TopicPartitionMap[Option[Offset]]]
  ): TopicPartitionMap[Option[OffsetRange]] = {
    val et: TopicPartitionMap[Option[Offset]] =
      to.fold(end)(end.leftCombine(_)((e, t) => t.orElse(e)))
    consumer_offsetRange(start, et)
  }
}
