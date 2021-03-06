package com.github.chenharryhua.nanjin.messages.kafka

import akka.kafka.ConsumerMessage.{
  CommittableMessage as AkkaConsumerMessage,
  CommittableOffset as AkkaCommittableOffset,
  GroupTopicPartition as AkkaGroupTopicPartition,
  PartitionOffset as AkkaPartitionOffset,
  TransactionalMessage as AkkaTransactionalMessage
}
import akka.kafka.ProducerMessage.{Message as AkkaProducerMessage, MultiMessage as AkkaMultiMessage}
import cats.Eq
import cats.syntax.all.*
import fs2.kafka.{
  ProducerRecords as Fs2ProducerRecords,
  TransactionalProducerRecords as Fs2TransactionalProducerRecords
}
import org.apache.kafka.clients.consumer.{ConsumerRecord, OffsetAndMetadata}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.header.{Header, Headers}

import java.util.Optional
import scala.compat.java8.OptionConverters.*

private[kafka] trait EqMessage {

  // kafka
  implicit val eqHeader: Eq[Header] = (x: Header, y: Header) =>
    (x.key() === y.key()) && (x.value().sameElements(y.value()))

  implicit val eqHeaders: Eq[Headers] = (x: Headers, y: Headers) => {
    val xa = x.toArray
    val ya = y.toArray
    (xa.size === ya.size) && xa.zip(ya).forall { case (x, y) => x === y }
  }

  implicit val eqOptionalInteger: Eq[Optional[java.lang.Integer]] =
    (x: Optional[Integer], y: Optional[Integer]) =>
      x.asScala.flatMap(Option(_).map(_.toInt)) === y.asScala.flatMap(Option(_).map(_.toInt))

  implicit val eqTopicPartition: Eq[TopicPartition] =
    (x: TopicPartition, y: TopicPartition) => x.equals(y)

  implicit val eqOffsetAndMetadata: Eq[OffsetAndMetadata] =
    (x: OffsetAndMetadata, y: OffsetAndMetadata) => x.equals(y)

  implicit final def eqConsumerRecord[K: Eq, V: Eq]: Eq[ConsumerRecord[K, V]] =
    (x: ConsumerRecord[K, V], y: ConsumerRecord[K, V]) =>
      (x.topic() === y.topic) &&
        (x.partition() === y.partition()) &&
        (x.offset() === y.offset()) &&
        (x.timestamp() === y.timestamp()) &&
        (x.timestampType().id === y.timestampType().id) &&
        (x.serializedKeySize() === y.serializedKeySize()) &&
        (x.serializedValueSize() === y.serializedValueSize()) &&
        (x.key() === y.key()) &&
        (x.value() === y.value()) &&
        (x.headers() === y.headers()) &&
        (x.leaderEpoch() === y.leaderEpoch())

  implicit final def eqProducerRecord[K: Eq, V: Eq]: Eq[ProducerRecord[K, V]] =
    (x: ProducerRecord[K, V], y: ProducerRecord[K, V]) =>
      (x.topic() === y.topic()) &&
        x.partition().equals(y.partition()) &&
        x.timestamp().equals(y.timestamp()) &&
        (x.key() === y.key()) &&
        (x.value() === y.value()) &&
        (x.headers() === y.headers())

  // akka
  implicit val eqGroupTopicPartitionAkka: Eq[AkkaGroupTopicPartition] =
    cats.derived.semiauto.eq[AkkaGroupTopicPartition]

  implicit val eqPartitionOffsetAkka: Eq[AkkaPartitionOffset] =
    cats.derived.semiauto.eq[AkkaPartitionOffset]

  implicit val eqCommittableOffsetAkka: Eq[AkkaCommittableOffset] =
    (x: AkkaCommittableOffset, y: AkkaCommittableOffset) => x.partitionOffset === y.partitionOffset

  implicit def eqCommittableMessageAkka[K: Eq, V: Eq]: Eq[AkkaConsumerMessage[K, V]] =
    cats.derived.semiauto.eq[AkkaConsumerMessage[K, V]]

  implicit def eqProducerMessageAkka[K: Eq, V: Eq, P: Eq]: Eq[AkkaProducerMessage[K, V, P]] =
    cats.derived.semiauto.eq[AkkaProducerMessage[K, V, P]]

  implicit def eqProducerMultiMessageAkka[K: Eq, V: Eq, P: Eq]: Eq[AkkaMultiMessage[K, V, P]] =
    (x: AkkaMultiMessage[K, V, P], y: AkkaMultiMessage[K, V, P]) =>
      (x.records.toList === y.records.toList) && (x.passThrough === y.passThrough)

  implicit def eqTransactionalMessageAkka[K: Eq, V: Eq]: Eq[AkkaTransactionalMessage[K, V]] =
    cats.derived.semiauto.eq[AkkaTransactionalMessage[K, V]]

  // fs2
  implicit final def eqProducerRecordsFs2[K: Eq, V: Eq, P: Eq]: Eq[Fs2ProducerRecords[K, V, P]] =
    (x: Fs2ProducerRecords[K, V, P], y: Fs2ProducerRecords[K, V, P]) =>
      (x.records === y.records) &&
        (x.passthrough === y.passthrough)

  implicit final def eqTransactionalProducerRecordsFs2[F[_], K: Eq, V: Eq, P: Eq]
    : Eq[Fs2TransactionalProducerRecords[F, K, V, P]] =
    (x: Fs2TransactionalProducerRecords[F, K, V, P], y: Fs2TransactionalProducerRecords[F, K, V, P]) =>
      (x.records === y.records) && (x.passthrough === y.passthrough)
}
