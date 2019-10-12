package com.github.chenharryhua.nanjin.codec

import akka.NotUsed
import akka.kafka.ProducerMessage.{Message => AkkaMessage}
import cats.Bitraverse
import fs2.kafka.{
  CommittableOffset,
  ProducerRecord  => Fs2ProducerRecord,
  ProducerRecords => Fs2ProducerRecords
}
import org.apache.kafka.clients.producer.ProducerRecord

sealed abstract class KafkaGenericEncoder[F[_, _]: Bitraverse, K, V] {
  def build(topicName: String, k: K, v: V): F[K, V]
}

object KafkaGenericEncoder {

  implicit def kafkaProducerRecordEncoder[K, V]: KafkaGenericEncoder[ProducerRecord, K, V] =
    new KafkaGenericEncoder[ProducerRecord, K, V] {

      override def build(topicName: String, k: K, v: V): ProducerRecord[K, V] =
        new ProducerRecord[K, V](topicName, k, v)
    }

  implicit def fs2ProducerRecordEncoder[K, V]: KafkaGenericEncoder[Fs2ProducerRecord, K, V] =
    new KafkaGenericEncoder[Fs2ProducerRecord, K, V] {

      override def build(topicName: String, k: K, v: V): Fs2ProducerRecord[K, V] =
        Fs2ProducerRecord[K, V](topicName, k, v)
    }

  implicit def fs2ProducerRecordsEncoder[F[_], K, V]
    : KafkaGenericEncoder[Fs2ProducerRecords[*, *, Option[CommittableOffset[F]]], K, V] =
    new KafkaGenericEncoder[Fs2ProducerRecords[*, *, Option[CommittableOffset[F]]], K, V] {

      override def build(
        topicName: String,
        k: K,
        v: V): Fs2ProducerRecords[K, V, Option[CommittableOffset[F]]] =
        Fs2ProducerRecords.one(fs2ProducerRecordEncoder.build(topicName, k, v), None)
    }

  implicit def akkaMessageEncoder[K, V]: KafkaGenericEncoder[AkkaMessage[*, *, NotUsed], K, V] =
    new KafkaGenericEncoder[AkkaMessage[*, *, NotUsed], K, V] {

      override def build(topicName: String, k: K, v: V): AkkaMessage[K, V, NotUsed] =
        AkkaMessage(kafkaProducerRecordEncoder.build(topicName, k, v), NotUsed)
    }
}
