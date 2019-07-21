package com.github.chenharryhua.nanjin.kafka

import cats.{Bifunctor, Monad, Traverse}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord

trait KafkaMessageBifunctorInstances {
  implicit final val consumerRecordBifunctor: Bifunctor[ConsumerRecord[?, ?]] =
    new Bifunctor[ConsumerRecord[?, ?]] {
      override def bimap[K1, V1, K2, V2](
        cr: ConsumerRecord[K1, V1]
      )(k: K1 => K2, v: V1 => V2): ConsumerRecord[K2, V2] =
        new ConsumerRecord[K2, V2](
          cr.topic,
          cr.partition,
          cr.offset,
          cr.timestamp,
          cr.timestampType,
          -1L, //just make compiler happy.. checksum is deprecated.
          cr.serializedKeySize,
          cr.serializedValueSize,
          k(cr.key),
          v(cr.value)
        )
    }
  implicit final val producerRecordBifunctor: Bifunctor[ProducerRecord[?, ?]] =
    new Bifunctor[ProducerRecord[?, ?]] {
      override def bimap[K1, V1, K2, V2](
        pr: ProducerRecord[K1, V1])(k: K1 => K2, v: V1 => V2): ProducerRecord[K2, V2] =
        new ProducerRecord[K2, V2](
          pr.topic(),
          pr.partition(),
          pr.timestamp(),
          k(pr.key()),
          v(pr.value()),
          pr.headers()
        )
    }
}

trait Fs2MessageBifunctor extends KafkaMessageBifunctorInstances {
  import fs2.kafka.{CommittableMessage, ProducerMessage, ProducerRecord}
  implicit final def fs2CommittableMessageBifunctor[F[_]]: Bifunctor[CommittableMessage[F, ?, ?]] =
    new Bifunctor[CommittableMessage[F, ?, ?]] {
      override def bimap[K1, V1, K2, V2](
        cm: CommittableMessage[F, K1, V1]
      )(k: K1 => K2, v: V1 => V2): CommittableMessage[F, K2, V2] =
        CommittableMessage[F, K2, V2](
          Bifunctor[ConsumerRecord].bimap(cm.record)(k, v),
          cm.committableOffset)
    }
  implicit final val fs2ProducerRecordBiFunctor: Bifunctor[ProducerRecord[?, ?]] =
    new Bifunctor[ProducerRecord[?, ?]] {
      override def bimap[K1, V1, K2, V2](
        m: ProducerRecord[K1, V1]
      )(k: K1 => K2, v: V1 => V2): ProducerRecord[K2, V2] =
        ProducerRecord[K2, V2](m.topic, k(m.key), v(m.value))
    }

  implicit final def fs2ProducerMessageBifunctor[F[+_]: Traverse, P](
    implicit M: Monad[F]
  ): Bifunctor[ProducerMessage[F, ?, ?, P]] =
    new Bifunctor[ProducerMessage[F, ?, ?, P]] {
      override def bimap[K1, V1, K2, V2](
        m: ProducerMessage[F, K1, V1, P]
      )(k: K1 => K2, v: V1 => V2): ProducerMessage[F, K2, V2, P] =
        ProducerMessage[F, K2, V2, P](M.map(m.records) { r =>
          Bifunctor[ProducerRecord].bimap(r)(k, v)
        }, m.passthrough)
    }
}

trait AkkaMessageBifunctor extends KafkaMessageBifunctorInstances {
  import akka.kafka.ConsumerMessage.CommittableMessage
  import akka.kafka.ProducerMessage.Message
  implicit final def akkaProducerMessageBifunctor[P]: Bifunctor[Message[?, ?, P]] =
    new Bifunctor[Message[?, ?, P]] {
      override def bimap[K1, V1, K2, V2](
        m: Message[K1, V1, P]
      )(k: K1 => K2, v: V1 => V2): Message[K2, V2, P] =
        new Message[K2, V2, P](Bifunctor[ProducerRecord].bimap(m.record)(k, v), m.passThrough)
    }

  implicit final val akkaCommittableMessageBifunctor: Bifunctor[CommittableMessage[?, ?]] =
    new Bifunctor[CommittableMessage[?, ?]] {
      override def bimap[K1, V1, K2, V2](
        cm: CommittableMessage[K1, V1]
      )(k: K1 => K2, v: V1 => V2): CommittableMessage[K2, V2] =
        cm.copy(record = Bifunctor[ConsumerRecord].bimap(cm.record)(k, v))
    }
}
