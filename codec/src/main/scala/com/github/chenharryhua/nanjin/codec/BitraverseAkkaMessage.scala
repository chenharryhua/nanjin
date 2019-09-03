package com.github.chenharryhua.nanjin.codec
import cats.implicits._
import cats.{Applicative, Bitraverse, Eq, Eval}
import akka.kafka.ConsumerMessage.{
  CommittableMessage,
  CommittableOffset,
  GroupTopicPartition,
  PartitionOffset
}
import akka.kafka.ProducerMessage.Message

trait BitraverseAkkaMessage extends BitraverseKafkaRecord {

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

  implicit final def bitraverseAkkaProducerMessage[P]: Bitraverse[Message[?, ?, P]] =
    new Bitraverse[Message[?, ?, P]] {

      override def bitraverse[G[_]: Applicative, A, B, C, D](
        fab: Message[A, B, P])(f: A => G[C], g: B => G[D]): G[Message[C, D, P]] =
        fab.record.bitraverse(f, g).map(r => fab.copy(record = r))

      override def bifoldLeft[A, B, C](fab: Message[A, B, P], c: C)(
        f: (C, A) => C,
        g: (C, B) => C): C = fab.record.bifoldLeft(c)(f, g)

      override def bifoldRight[A, B, C](fab: Message[A, B, P], c: Eval[C])(
        f: (A, Eval[C]) => Eval[C],
        g: (B, Eval[C]) => Eval[C]): Eval[C] = fab.record.bifoldRight(c)(f, g)
    }

  implicit final val bitraverseAkkaCommittableMessage: Bitraverse[CommittableMessage] =
    new Bitraverse[CommittableMessage] {
      override def bitraverse[G[_]: Applicative, A, B, C, D](
        fab: CommittableMessage[A, B])(f: A => G[C], g: B => G[D]): G[CommittableMessage[C, D]] =
        fab.record.bitraverse(f, g).map(r => fab.copy(record = r))

      override def bifoldLeft[A, B, C](fab: CommittableMessage[A, B], c: C)(
        f: (C, A) => C,
        g: (C, B) => C): C = fab.record.bifoldLeft(c)(f, g)

      override def bifoldRight[A, B, C](fab: CommittableMessage[A, B], c: Eval[C])(
        f: (A, Eval[C]) => Eval[C],
        g: (B, Eval[C]) => Eval[C]): Eval[C] = fab.record.bifoldRight(c)(f, g)
    }
}
