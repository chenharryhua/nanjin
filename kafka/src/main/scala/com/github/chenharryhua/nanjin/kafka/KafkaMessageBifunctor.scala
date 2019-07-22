package com.github.chenharryhua.nanjin.kafka

import cats.{Bifunctor, Traverse}
import com.github.ghik.silencer.silent
import monocle.Iso
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord

trait KafkaMessageBifunctor {
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
          cr.checksum: @silent,
          cr.serializedKeySize,
          cr.serializedValueSize,
          k(cr.key),
          v(cr.value))
    }
  implicit final val producerRecordBifunctor: Bifunctor[ProducerRecord[?, ?]] =
    new Bifunctor[ProducerRecord[?, ?]] {
      override def bimap[K1, V1, K2, V2](
        pr: ProducerRecord[K1, V1])(k: K1 => K2, v: V1 => V2): ProducerRecord[K2, V2] =
        new ProducerRecord[K2, V2](
          pr.topic,
          pr.partition,
          pr.timestamp,
          k(pr.key),
          v(pr.value),
          pr.headers)
    }

  final def exactlyCopy[K, V](rec: ConsumerRecord[K, V]): ProducerRecord[K, V] =
    new ProducerRecord(rec.topic, rec.partition, rec.timestamp, rec.key, rec.value, rec.headers)

  final def fromConsumerRecord[K, V](
    toTopic: String,
    rec: ConsumerRecord[K, V]): ProducerRecord[K, V] =
    new ProducerRecord(toTopic, null, rec.timestamp, rec.key, rec.value, rec.headers)
}

trait Fs2MessageBifunctor extends KafkaMessageBifunctor {

  import fs2.kafka.{
    CommittableMessage,
    Headers,
    ProducerMessage,
    ProducerRecord => Fs2ProducerRecord
  }

  implicit final def fs2CommittableMessageBifunctor[F[_]]: Bifunctor[CommittableMessage[F, ?, ?]] =
    new Bifunctor[CommittableMessage[F, ?, ?]] {
      override def bimap[K1, V1, K2, V2](
        cm: CommittableMessage[F, K1, V1]
      )(k: K1 => K2, v: V1 => V2): CommittableMessage[F, K2, V2] =
        CommittableMessage[F, K2, V2](
          Bifunctor[ConsumerRecord].bimap(cm.record)(k, v),
          cm.committableOffset)
    }

  implicit final val fs2ProducerRecordBiFunctor: Bifunctor[Fs2ProducerRecord[?, ?]] =
    new Bifunctor[Fs2ProducerRecord[?, ?]] {
      override def bimap[K1, V1, K2, V2](
        m: Fs2ProducerRecord[K1, V1]
      )(k: K1 => K2, v: V1 => V2): Fs2ProducerRecord[K2, V2] = {
        val pr  = Fs2ProducerRecord[K2, V2](m.topic, k(m.key), v(m.value)).withHeaders(m.headers)
        val ppr = m.partition.fold(pr)(p => pr.withPartition(p))
        m.timestamp.fold(ppr)(t => ppr.withTimestamp(t))
      }
    }

  implicit final def fs2ProducerMessageBifunctor[F[+_]: Traverse, P]
    : Bifunctor[ProducerMessage[F, ?, ?, P]] =
    new Bifunctor[ProducerMessage[F, ?, ?, P]] {
      override def bimap[K1, V1, K2, V2](
        m: ProducerMessage[F, K1, V1, P]
      )(k: K1 => K2, v: V1 => V2): ProducerMessage[F, K2, V2, P] = {
        ProducerMessage[F, K2, V2, P](Traverse[F].map(m.records) { r =>
          Bifunctor[Fs2ProducerRecord].bimap(r)(k, v)
        }, m.passthrough)
      }
    }

  final def fs2ProducerRecordIso[K, V]: Iso[Fs2ProducerRecord[K, V], ProducerRecord[K, V]] =
    Iso[Fs2ProducerRecord[K, V], ProducerRecord[K, V]](
      s =>
        new ProducerRecord[K, V](
          s.topic,
          s.partition.map(new java.lang.Integer(_)).orNull,
          s.timestamp.map(new java.lang.Long(_)).orNull,
          s.key,
          s.value,
          s.headers.asJava))(a =>
      Fs2ProducerRecord(a.topic, a.key, a.value)
        .withPartition(a.partition)
        .withTimestamp(a.timestamp)
        .withHeaders(a.headers.toArray.foldLeft(Headers.empty)((t, i) => t.append(i.key, i.value))))

}

trait AkkaMessageBifunctor extends KafkaMessageBifunctor {
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
