package com.github.chenharryhua.nanjin.kafka

import cats.{Bifunctor, Monad, Traverse}
import com.github.ghik.silencer.silent
import monocle.{Iso, Lens}
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

  final def consumerRecordKeyLens[K, V]: Lens[ConsumerRecord[K, V], K] =
    Lens[ConsumerRecord[K, V], K](_.key)(k =>
      s => Bifunctor[ConsumerRecord].bimap(s)(_ => k, identity))

  final def consumerRecordValueLens[K, V]: Lens[ConsumerRecord[K, V], V] =
    Lens[ConsumerRecord[K, V], V](_.value)(v =>
      s => Bifunctor[ConsumerRecord].bimap(s)(identity, _ => v))

  final def producerRecordKeyLens[K, V]: Lens[ProducerRecord[K, V], K] =
    Lens[ProducerRecord[K, V], K](_.key)(k =>
      s => Bifunctor[ProducerRecord].bimap(s)(_ => k, identity))

  final def producerRecordValueLens[K, V]: Lens[ProducerRecord[K, V], V] =
    Lens[ProducerRecord[K, V], V](_.value)(v =>
      s => Bifunctor[ProducerRecord].bimap(s)(identity, _ => v))
}

trait Fs2MessageBifunctor extends KafkaMessageBifunctor {

  import fs2.kafka.{CommittableMessage, Id, ProducerMessage, ProducerRecord => Fs2ProducerRecord}

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
      )(k: K1 => K2, v: V1 => V2): Fs2ProducerRecord[K2, V2] =
        Fs2ProducerRecord[K2, V2](m.topic, k(m.key), v(m.value))
    }

  implicit final def fs2ProducerMessageBifunctor[F[+_]: Traverse, P](
    implicit M: Monad[F]
  ): Bifunctor[ProducerMessage[F, ?, ?, P]] =
    new Bifunctor[ProducerMessage[F, ?, ?, P]] {
      override def bimap[K1, V1, K2, V2](
        m: ProducerMessage[F, K1, V1, P]
      )(k: K1 => K2, v: V1 => V2): ProducerMessage[F, K2, V2, P] =
        ProducerMessage[F, K2, V2, P](M.map(m.records) { r =>
          Bifunctor[Fs2ProducerRecord].bimap(r)(k, v)
        }, m.passthrough)
    }

  final def fs2ConsumerRecordLens[F[_], K, V]
    : Lens[CommittableMessage[F, K, V], ConsumerRecord[K, V]] =
    Lens[CommittableMessage[F, K, V], ConsumerRecord[K, V]](_.record)(r =>
      s => CommittableMessage[F, K, V](r, s.committableOffset))

  final def fs2ConsumerRecordKeyLens[F[_], K, V]: Lens[CommittableMessage[F, K, V], K] =
    fs2ConsumerRecordLens[F, K, V].composeLens(super.consumerRecordKeyLens[K, V])

  final def fs2ConsumerRecordValueLens[F[_], K, V]: Lens[CommittableMessage[F, K, V], V] =
    fs2ConsumerRecordLens[F, K, V].composeLens(super.consumerRecordValueLens[K, V])

  final def fs2ProducerRecordLens[F[+_]: Traverse, K, V, P]
    : Lens[ProducerMessage[F, K, V, P], F[Fs2ProducerRecord[K, V]]] =
    Lens[ProducerMessage[F, K, V, P], F[Fs2ProducerRecord[K, V]]](_.records)(r =>
      s => ProducerMessage[F, K, V, P](r, s.passthrough))

  private def iso[K, V]: Iso[Fs2ProducerRecord[K, V], ProducerRecord[K, V]] =
    Iso[Fs2ProducerRecord[K, V], ProducerRecord[K, V]](s =>
      new ProducerRecord[K, V](s.topic, s.key, s.value))(a =>
      Fs2ProducerRecord(a.topic, a.key, a.value))

  final def fs2ProducerRecordKeyLens[K, V, P]: Lens[ProducerMessage[Id, K, V, P], K] =
    fs2ProducerRecordLens[Id, K, V, P].composeIso(iso).composeLens(super.producerRecordKeyLens)

  final def fs2ProducerRecordValueLens[K, V, P]: Lens[ProducerMessage[Id, K, V, P], V] =
    fs2ProducerRecordLens[Id, K, V, P].composeIso(iso).composeLens(super.producerRecordValueLens)
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

  final def akkaConsumerRecordLens[K, V]: Lens[CommittableMessage[K, V], ConsumerRecord[K, V]] =
    Lens[CommittableMessage[K, V], ConsumerRecord[K, V]](_.record)(r =>
      s => CommittableMessage[K, V](r, s.committableOffset))

  final def akkaConsumerRecordKeyLens[K, V]: Lens[CommittableMessage[K, V], K] =
    akkaConsumerRecordLens[K, V].composeLens(super.consumerRecordKeyLens[K, V])

  final def akkaConsumerRecordValueLens[K, V]: Lens[CommittableMessage[K, V], V] =
    akkaConsumerRecordLens[K, V].composeLens(super.consumerRecordValueLens[K, V])

  final def akkaProducerRecordLens[K, V, P]: Lens[Message[K, V, P], ProducerRecord[K, V]] =
    Lens[Message[K, V, P], ProducerRecord[K, V]](_.record)(r =>
      s => Message[K, V, P](r, s.passThrough))

  final def akkaProducerRecordKeyLens[K, V, P]: Lens[Message[K, V, P], K] =
    akkaProducerRecordLens[K, V, P].composeLens(super.producerRecordKeyLens[K, V])

  final def akkaProducerRecordValueLens[K, V, P]: Lens[Message[K, V, P], V] =
    akkaProducerRecordLens[K, V, P].composeLens(super.producerRecordValueLens[K, V])
}
