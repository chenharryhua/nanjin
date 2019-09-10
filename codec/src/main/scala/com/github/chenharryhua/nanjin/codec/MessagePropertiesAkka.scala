package com.github.chenharryhua.nanjin.codec
import akka.kafka.ConsumerMessage.{
  CommittableMessage,
  CommittableOffset,
  GroupTopicPartition,
  PartitionOffset
}
import akka.kafka.ProducerMessage.Message
import cats.Eq
import cats.implicits._

trait MessagePropertiesAkka extends BitraverseKafkaRecord {
  implicit val eqGroupTopicPartitionAkka: Eq[GroupTopicPartition] =
    cats.derived.semi.eq[GroupTopicPartition]
  implicit val eqPartitionOffsetAkka: Eq[PartitionOffset] =
    cats.derived.semi.eq[PartitionOffset]
  implicit val eqCommittableOffsetAkka: Eq[CommittableOffset] =
    (x: CommittableOffset, y: CommittableOffset) => x.partitionOffset === y.partitionOffset
  implicit def eqCommittableMessageAkka[K: Eq, V: Eq]: Eq[CommittableMessage[K, V]] =
    cats.derived.semi.eq[CommittableMessage[K, V]]
  implicit def eqProducerMessageAkka[K: Eq, V: Eq, P: Eq]: Eq[Message[K, V, P]] =
    cats.derived.semi.eq[Message[K, V, P]]
}
