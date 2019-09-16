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

sealed trait BitraverseMessage[F[_, _]] extends Bitraverse[F] {
  type H[_, _]
  implicit def baseInst: Bitraverse[H]
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

object BitraverseMessage extends BitraverseKafkaRecord {

  def apply[F[_, _]](
    implicit ev: BitraverseMessage[F]): BitraverseMessage[F] { type H[A, B] = ev.H[A, B] } =
    ev

  implicit val identityConsumerRecord
    : BitraverseMessage[ConsumerRecord] { type H[A, B] = ConsumerRecord[A, B] } =
    new BitraverseMessage[ConsumerRecord] {
      override type H[K, V] = ConsumerRecord[K, V]
      override val baseInst: Bitraverse[ConsumerRecord] = bitraverseConsumerRecord
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

  implicit val fs2ConsumerRecord
    : BitraverseMessage[Fs2ConsumerRecord] { type H[A, B] = ConsumerRecord[A, B] } =
    new BitraverseMessage[Fs2ConsumerRecord] {
      override type H[K, V] = ConsumerRecord[K, V]
      override val baseInst: Bitraverse[ConsumerRecord] = bitraverseConsumerRecord
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

  implicit val akkaCommittableMessage
    : BitraverseMessage[AkkaCommittableMessage] { type H[A, B] = ConsumerRecord[A, B] } =
    new BitraverseMessage[AkkaCommittableMessage] {
      override type H[K, V] = ConsumerRecord[K, V]
      override val baseInst: Bitraverse[ConsumerRecord] = bitraverseConsumerRecord
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

  implicit val akkaAkkaTransactionalMessage
    : BitraverseMessage[AkkaTransactionalMessage] { type H[A, B] = ConsumerRecord[A, B] } =
    new BitraverseMessage[AkkaTransactionalMessage] {
      override type H[K, V] = ConsumerRecord[K, V]
      override val baseInst: Bitraverse[ConsumerRecord] = bitraverseConsumerRecord
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

  implicit def fs2CommittableConsumerRecord[F[_]]
    : BitraverseMessage[Fs2CommittableConsumerRecord[F, *, *]] {
      type H[A, B] = ConsumerRecord[A, B]
    } =
    new BitraverseMessage[Fs2CommittableConsumerRecord[F, *, *]] {
      override type H[K, V] = ConsumerRecord[K, V]
      override val baseInst: Bitraverse[ConsumerRecord] = bitraverseConsumerRecord
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

  implicit val identityProducerRecord
    : BitraverseMessage[ProducerRecord] { type H[A, B] = ProducerRecord[A, B] } =
    new BitraverseMessage[ProducerRecord] {
      override type H[K, V] = ProducerRecord[K, V]
      override val baseInst: Bitraverse[ProducerRecord] = bitraverseProducerRecord
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

  implicit def fs2ProducerRecord[P]
    : BitraverseMessage[Fs2ProducerRecord] { type H[A, B] = ProducerRecord[A, B] } =
    new BitraverseMessage[Fs2ProducerRecord] {
      override type H[K, V] = ProducerRecord[K, V]
      override val baseInst: Bitraverse[ProducerRecord] = bitraverseProducerRecord
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

  implicit def akkaProducerMessage[P]
    : BitraverseMessage[AkkaProducerMessage[*, *, P]] { type H[A, B] = ProducerRecord[A, B] } =
    new BitraverseMessage[AkkaProducerMessage[*, *, P]] {
      override type H[K, V] = ProducerRecord[K, V]
      override val baseInst: Bitraverse[ProducerRecord] = bitraverseProducerRecord
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
