package com.github.chenharryhua.nanjin.codec

import akka.NotUsed
import akka.kafka.ProducerMessage.{Message => AkkaMessage}
import fs2.kafka.{ProducerRecord           => Fs2ProducerRecord, ProducerRecords => Fs2ProducerRecords}
import org.apache.kafka.clients.producer.ProducerRecord

sealed abstract class KafkaGenericEncoder[F[_, _]] {
  def single[K, V](topicName: String, k: K, v: V): F[K, V]
}

object KafkaGenericEncoder {

  implicit def kafkaProducerRecordEncoder[K, V]: KafkaGenericEncoder[ProducerRecord] =
    new KafkaGenericEncoder[ProducerRecord] {

      override def single[K, V](topicName: String, k: K, v: V): ProducerRecord[K, V] =
        new ProducerRecord[K, V](topicName, k, v)
    }

  implicit def fs2ProducerRecordEncoder[K, V]: KafkaGenericEncoder[Fs2ProducerRecord] =
    new KafkaGenericEncoder[Fs2ProducerRecord] {

      override def single[K, V](topicName: String, k: K, v: V): Fs2ProducerRecord[K, V] =
        Fs2ProducerRecord[K, V](topicName, k, v)
    }

  implicit def akkaMessageEncoder[K, V]: KafkaGenericEncoder[AkkaMessage[*, *, NotUsed]] =
    new KafkaGenericEncoder[AkkaMessage[*, *, NotUsed]] {

      override def single[K, V](topicName: String, k: K, v: V): AkkaMessage[K, V, NotUsed] =
        AkkaMessage(kafkaProducerRecordEncoder.single(topicName, k, v), NotUsed)
    }

  implicit def fs2ProducerRecordsEncoder[F[_]]
    : KafkaGenericEncoder[Fs2ProducerRecords[*, *, Unit]] =
    new KafkaGenericEncoder[Fs2ProducerRecords[*, *, Unit]] {

      override def single[K, V](topicName: String, k: K, v: V): Fs2ProducerRecords[K, V, Unit] =
        Fs2ProducerRecords.one(fs2ProducerRecordEncoder.single(topicName, k, v))
    }
}
