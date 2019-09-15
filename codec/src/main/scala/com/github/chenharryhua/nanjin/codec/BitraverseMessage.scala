package com.github.chenharryhua.nanjin.codec

import akka.kafka.ConsumerMessage.{
  CommittableMessage   => AkkaCommittableMessage,
  TransactionalMessage => AkkaTransactionalMessage
}
import akka.kafka.ProducerMessage.{Message => AkkaProducerMessage}
import cats.implicits._
import cats.{Applicative, Bitraverse, Eval}
import fs2.kafka.{
  CommittableConsumerRecord => Fs2CommittableConsumerRecord,
  ConsumerRecord            => Fs2ConsumerRecord,
  ProducerRecord            => Fs2ProducerRecord
}
import monocle.PLens
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord

sealed abstract class BitraverseMessage[F[_, _], H[_, _]: Bitraverse] extends Bitraverse[F] {

  def lens[K1, V1, K2, V2]: PLens[F[K1, V1], F[K2, V2], H[K1, V1], H[K2, V2]]

  final override def bitraverse[G[_], A, B, C, D](fab: F[A, B])(f: A => G[C], g: B => G[D])(
    implicit G: Applicative[G]): G[F[C, D]] =
    lens.modifyF((cr: H[A, B]) => cr.bitraverse(f, g))(fab)

  final override def bifoldLeft[A, B, C](fab: F[A, B], c: C)(f: (C, A) => C, g: (C, B) => C): C =
    lens.get(fab).bifoldLeft(c)(f, g)

  final override def bifoldRight[A, B, C](fab: F[A, B], c: Eval[C])(
    f: (A, Eval[C]) => Eval[C],
    g: (B, Eval[C]) => Eval[C]): Eval[C] =
    lens.get(fab).bifoldRight(c)(f, g)
}

object BitraverseMessage {
  def apply[F[_, _], H[_, _]](implicit ev: BitraverseMessage[F, H]): BitraverseMessage[F, H] = ev

  implicit val identityConsumerRecordLike: BitraverseMessage[ConsumerRecord, ConsumerRecord] =
    new BitraverseMessage[ConsumerRecord, ConsumerRecord] {
      override def lens[K1, V1, K2, V2]: PLens[
        ConsumerRecord[K1, V1],
        ConsumerRecord[K2, V2],
        ConsumerRecord[K1, V1],
        ConsumerRecord[K2, V2]] =
        PLens[
          ConsumerRecord[K1, V1],
          ConsumerRecord[K2, V2],
          ConsumerRecord[K1, V1],
          ConsumerRecord[K2, V2]](s => s)(b => _ => b)
    }

  implicit val fs2ConsumerRecordLike: BitraverseMessage[Fs2ConsumerRecord, ConsumerRecord] =
    new BitraverseMessage[Fs2ConsumerRecord, ConsumerRecord] {
      override def lens[K1, V1, K2, V2]: PLens[
        Fs2ConsumerRecord[K1, V1],
        Fs2ConsumerRecord[K2, V2],
        ConsumerRecord[K1, V1],
        ConsumerRecord[K2, V2]] =
        PLens[
          Fs2ConsumerRecord[K1, V1],
          Fs2ConsumerRecord[K2, V2],
          ConsumerRecord[K1, V1],
          ConsumerRecord[K2, V2]](isoFs2ComsumerRecord.get) { b => _ =>
          isoFs2ComsumerRecord.reverseGet(b)
        }
    }

  implicit val akkaConsumerMessageLike: BitraverseMessage[AkkaCommittableMessage, ConsumerRecord] =
    new BitraverseMessage[AkkaCommittableMessage, ConsumerRecord] {
      override def lens[K1, V1, K2, V2]: PLens[
        AkkaCommittableMessage[K1, V1],
        AkkaCommittableMessage[K2, V2],
        ConsumerRecord[K1, V1],
        ConsumerRecord[K2, V2]] =
        PLens[
          AkkaCommittableMessage[K1, V1],
          AkkaCommittableMessage[K2, V2],
          ConsumerRecord[K1, V1],
          ConsumerRecord[K2, V2]](_.record)(b => s => s.copy(record = b))
    }

  implicit val akkaAkkaTransactionalMessageLike
    : BitraverseMessage[AkkaTransactionalMessage, ConsumerRecord] =
    new BitraverseMessage[AkkaTransactionalMessage, ConsumerRecord] {
      override def lens[K1, V1, K2, V2]: PLens[
        AkkaTransactionalMessage[K1, V1],
        AkkaTransactionalMessage[K2, V2],
        ConsumerRecord[K1, V1],
        ConsumerRecord[K2, V2]] =
        PLens[
          AkkaTransactionalMessage[K1, V1],
          AkkaTransactionalMessage[K2, V2],
          ConsumerRecord[K1, V1],
          ConsumerRecord[K2, V2]](_.record)(b => s => s.copy(record = b))
    }

  implicit def fs2ConsumerMessageLike[F[_]]
    : BitraverseMessage[Fs2CommittableConsumerRecord[F, *, *], ConsumerRecord] =
    new BitraverseMessage[Fs2CommittableConsumerRecord[F, *, *], ConsumerRecord] {
      override def lens[K1, V1, K2, V2]: PLens[
        Fs2CommittableConsumerRecord[F, K1, V1],
        Fs2CommittableConsumerRecord[F, K2, V2],
        ConsumerRecord[K1, V1],
        ConsumerRecord[K2, V2]] =
        PLens[
          Fs2CommittableConsumerRecord[F, K1, V1],
          Fs2CommittableConsumerRecord[F, K2, V2],
          ConsumerRecord[K1, V1],
          ConsumerRecord[K2, V2]](cm => isoFs2ComsumerRecord.get(cm.record)) { b => s =>
          Fs2CommittableConsumerRecord(isoFs2ComsumerRecord.reverseGet(b), s.offset)
        }
    }

  implicit val identityProducerRecordLike: BitraverseMessage[ProducerRecord, ProducerRecord] =
    new BitraverseMessage[ProducerRecord, ProducerRecord] {
      override def lens[K1, V1, K2, V2]: PLens[
        ProducerRecord[K1, V1],
        ProducerRecord[K2, V2],
        ProducerRecord[K1, V1],
        ProducerRecord[K2, V2]] =
        PLens[
          ProducerRecord[K1, V1],
          ProducerRecord[K2, V2],
          ProducerRecord[K1, V1],
          ProducerRecord[K2, V2]](s => s)(b => _ => b)
    }

  implicit def fs2ProducerRecordLike[P]: BitraverseMessage[Fs2ProducerRecord, ProducerRecord] =
    new BitraverseMessage[Fs2ProducerRecord, ProducerRecord] {
      override def lens[K1, V1, K2, V2]: PLens[
        Fs2ProducerRecord[K1, V1],
        Fs2ProducerRecord[K2, V2],
        ProducerRecord[K1, V1],
        ProducerRecord[K2, V2]] =
        PLens[
          Fs2ProducerRecord[K1, V1],
          Fs2ProducerRecord[K2, V2],
          ProducerRecord[K1, V1],
          ProducerRecord[K2, V2]](isoFs2ProducerRecord.get) { b => _ =>
          isoFs2ProducerRecord.reverseGet(b)
        }
    }

  implicit def akkaProducerMessageLike[P]
    : BitraverseMessage[AkkaProducerMessage[*, *, P], ProducerRecord] =
    new BitraverseMessage[AkkaProducerMessage[*, *, P], ProducerRecord] {
      override def lens[K1, V1, K2, V2]: PLens[
        AkkaProducerMessage[K1, V1, P],
        AkkaProducerMessage[K2, V2, P],
        ProducerRecord[K1, V1],
        ProducerRecord[K2, V2]] =
        PLens[
          AkkaProducerMessage[K1, V1, P],
          AkkaProducerMessage[K2, V2, P],
          ProducerRecord[K1, V1],
          ProducerRecord[K2, V2]](_.record)(b => s => s.copy(record = b))
    }
}
