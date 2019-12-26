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

private [codec] sealed trait BitraverseMessage[F[_, _]] extends Bitraverse[F] {
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

sealed trait NJConsumerMessage[F[_, _]] extends BitraverseMessage[F] {
  def record[K, V](cr: F[Option[K], Option[V]]): NJConsumerRecord[K, V]
}

object NJConsumerMessage extends BitraverseKafkaRecord {

  def apply[F[_, _]](
    implicit ev: NJConsumerMessage[F]): NJConsumerMessage[F] { type H[A, B] = ev.H[A, B] } = ev

  implicit val identityConsumerRecordBitraverseMessage
    : NJConsumerMessage[ConsumerRecord] { type H[A, B] = ConsumerRecord[A, B] } =
    new NJConsumerMessage[ConsumerRecord] {
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

      override def record[K, V](cr: ConsumerRecord[Option[K], Option[V]]): NJConsumerRecord[K, V] =
        NJConsumerRecord(cr)
    }

  implicit val fs2ConsumerRecordBitraverseMessage
    : NJConsumerMessage[Fs2ConsumerRecord] { type H[A, B] = ConsumerRecord[A, B] } =
    new NJConsumerMessage[Fs2ConsumerRecord] {
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
          ConsumerRecord[K2, V2]](iso.isoFs2ComsumerRecord.get) { b => _ =>
          iso.isoFs2ComsumerRecord.reverseGet(b)
        }

      override def record[K, V](
        cr: Fs2ConsumerRecord[Option[K], Option[V]]): NJConsumerRecord[K, V] =
        NJConsumerRecord(iso.isoFs2ComsumerRecord.get(cr))

    }

  implicit val akkaCommittableMessageBitraverseMessage
    : NJConsumerMessage[AkkaCommittableMessage] { type H[A, B] = ConsumerRecord[A, B] } =
    new NJConsumerMessage[AkkaCommittableMessage] {
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

      override def record[K, V](
        cr: AkkaCommittableMessage[Option[K], Option[V]]): NJConsumerRecord[K, V] =
        NJConsumerRecord(cr.record)

    }

  implicit val akkaAkkaTransactionalMessageBitraverseMessage
    : NJConsumerMessage[AkkaTransactionalMessage] { type H[A, B] = ConsumerRecord[A, B] } =
    new NJConsumerMessage[AkkaTransactionalMessage] {
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

      override def record[K, V](
        cr: AkkaTransactionalMessage[Option[K], Option[V]]): NJConsumerRecord[K, V] =
        NJConsumerRecord(cr.record)

    }

  implicit def fs2CommittableConsumerRecordBitraverseMessage[F[_]]
    : NJConsumerMessage[Fs2CommittableConsumerRecord[F, *, *]] {
      type H[A, B] = ConsumerRecord[A, B]
    } =
    new NJConsumerMessage[Fs2CommittableConsumerRecord[F, *, *]] {
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
          ConsumerRecord[K2, V2]](cm => iso.isoFs2ComsumerRecord.get(cm.record)) { b => s =>
          Fs2CommittableConsumerRecord(iso.isoFs2ComsumerRecord.reverseGet(b), s.offset)
        }

      override def record[K, V](
        cr: Fs2CommittableConsumerRecord[F, Option[K], Option[V]]): NJConsumerRecord[K, V] =
        NJConsumerRecord(iso.isoFs2ComsumerRecord.get(cr.record))
    }
}

sealed trait NJProducerMessage[F[_, _]] extends BitraverseMessage[F] {
  def record[K, V](cr: F[Option[K], Option[V]]): NJProducerRecord[K, V]
}

object NJProducerMessage extends BitraverseKafkaRecord {

  def apply[F[_, _]](
    implicit ev: NJProducerMessage[F]): NJProducerMessage[F] { type H[A, B] = ev.H[A, B] } = ev

  implicit val identityProducerRecordBitraverseMessage
    : NJProducerMessage[ProducerRecord] { type H[A, B] = ProducerRecord[A, B] } =
    new NJProducerMessage[ProducerRecord] {
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

      override def record[K, V](cr: ProducerRecord[Option[K], Option[V]]): NJProducerRecord[K, V] =
        NJProducerRecord(cr)
    }

  implicit val fs2ProducerRecordBitraverseMessage
    : NJProducerMessage[Fs2ProducerRecord] { type H[A, B] = ProducerRecord[A, B] } =
    new NJProducerMessage[Fs2ProducerRecord] {
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
          ProducerRecord[K2, V2]](iso.isoFs2ProducerRecord[K1, V1].get) { b => _ =>
          iso.isoFs2ProducerRecord[K2, V2].reverseGet(b)
        }

      override def record[K, V](
        cr: Fs2ProducerRecord[Option[K], Option[V]]): NJProducerRecord[K, V] =
        NJProducerRecord(iso.isoFs2ProducerRecord[Option[K], Option[V]].get(cr))
    }

  implicit def akkaProducerMessageBitraverseMessage[P]
    : NJProducerMessage[AkkaProducerMessage[*, *, P]] { type H[A, B] = ProducerRecord[A, B] } =
    new NJProducerMessage[AkkaProducerMessage[*, *, P]] {
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

      override def record[K, V](
        cr: AkkaProducerMessage[Option[K], Option[V], P]): NJProducerRecord[K, V] =
        NJProducerRecord(cr.record)
    }
}
