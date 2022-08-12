package com.github.chenharryhua.nanjin.messages.kafka

import akka.kafka.ConsumerMessage.{
  CommittableMessage as AkkaCommittableMessage,
  TransactionalMessage as AkkaTransactionalMessage
}
import akka.kafka.ProducerMessage.Message as AkkaProducerMessage
import cats.syntax.all.*
import cats.{Applicative, Bitraverse, Eval}
import com.github.chenharryhua.nanjin.messages.kafka.instances.*
import fs2.kafka.{
  CommittableConsumerRecord as Fs2CommittableConsumerRecord,
  ConsumerRecord as Fs2ConsumerRecord,
  ProducerRecord as Fs2ProducerRecord
}
import io.scalaland.chimney.dsl.*
import monocle.PLens
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord

sealed trait BitraverseMessage[F[_, _]] extends Bitraverse[F] {
  type H[_, _]
  implicit def baseInst: Bitraverse[H]
  def lens[K1, V1, K2, V2]: PLens[F[K1, V1], F[K2, V2], H[K1, V1], H[K2, V2]]

  final override def bitraverse[G[_], A, B, C, D](fab: F[A, B])(f: A => G[C], g: B => G[D])(implicit
    G: Applicative[G]): G[F[C, D]] =
    lens.modifyF((cr: H[A, B]) => cr.bitraverse(f, g))(fab)

  final override def bifoldLeft[A, B, C](fab: F[A, B], c: C)(f: (C, A) => C, g: (C, B) => C): C =
    lens.get(fab).bifoldLeft(c)(f, g)

  final override def bifoldRight[A, B, C](fab: F[A, B], c: Eval[C])(
    f: (A, Eval[C]) => Eval[C],
    g: (B, Eval[C]) => Eval[C]): Eval[C] =
    lens.get(fab).bifoldRight(c)(f, g)
}

sealed trait NJConsumerMessage[F[_, _]] extends BitraverseMessage[F] with BitraverseKafkaRecord {
  final override type H[K, V] = ConsumerRecord[K, V]
  final override val baseInst: Bitraverse[ConsumerRecord] = bitraverseConsumerRecord
}

object NJConsumerMessage {
  final type Aux[F[_, _]] = NJConsumerMessage[F] { type H[K, V] = ConsumerRecord[K, V] }

  def apply[F[_, _]](implicit ev: NJConsumerMessage[F]): Aux[F] = ev

  implicit val icrbi1: Aux[ConsumerRecord] =
    new NJConsumerMessage[ConsumerRecord] {

      override def lens[K1, V1, K2, V2]: PLens[
        ConsumerRecord[K1, V1],
        ConsumerRecord[K2, V2],
        ConsumerRecord[K1, V1],
        ConsumerRecord[K2, V2]] =
        PLens[ConsumerRecord[K1, V1], ConsumerRecord[K2, V2], ConsumerRecord[K1, V1], ConsumerRecord[K2, V2]](
          s => s)(b => _ => b)
    }

  implicit val icrbi2: Aux[Fs2ConsumerRecord] =
    new NJConsumerMessage[Fs2ConsumerRecord] {

      override def lens[K1, V1, K2, V2]: PLens[
        Fs2ConsumerRecord[K1, V1],
        Fs2ConsumerRecord[K2, V2],
        ConsumerRecord[K1, V1],
        ConsumerRecord[K2, V2]] =
        PLens[
          Fs2ConsumerRecord[K1, V1],
          Fs2ConsumerRecord[K2, V2],
          ConsumerRecord[K1, V1],
          ConsumerRecord[K2, V2]](_.transformInto)(b => _ => b.transformInto)

    }

  implicit val icrbi3: Aux[AkkaCommittableMessage] =
    new NJConsumerMessage[AkkaCommittableMessage] {

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

  implicit val icrbi4: Aux[AkkaTransactionalMessage] =
    new NJConsumerMessage[AkkaTransactionalMessage] {

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

  implicit def icrbi5[F[_]]: Aux[Fs2CommittableConsumerRecord[F, *, *]] =
    new NJConsumerMessage[Fs2CommittableConsumerRecord[F, *, *]] {

      override def lens[K1, V1, K2, V2]: PLens[
        Fs2CommittableConsumerRecord[F, K1, V1],
        Fs2CommittableConsumerRecord[F, K2, V2],
        ConsumerRecord[K1, V1],
        ConsumerRecord[K2, V2]] =
        PLens[
          Fs2CommittableConsumerRecord[F, K1, V1],
          Fs2CommittableConsumerRecord[F, K2, V2],
          ConsumerRecord[K1, V1],
          ConsumerRecord[K2, V2]](_.record.transformInto) { b => s =>
          Fs2CommittableConsumerRecord(b.transformInto, s.offset)
        }
    }
}

sealed trait NJProducerMessage[F[_, _]] extends BitraverseMessage[F] with BitraverseKafkaRecord {
  final override type H[K, V] = ProducerRecord[K, V]
  final override val baseInst: Bitraverse[ProducerRecord] = bitraverseProducerRecord
}

object NJProducerMessage {
  final type Aux[F[_, _]] = NJProducerMessage[F] { type H[K, V] = ProducerRecord[K, V] }

  def apply[F[_, _]](implicit ev: NJProducerMessage[F]): Aux[F] = ev

  implicit val iprbi1: Aux[ProducerRecord] =
    new NJProducerMessage[ProducerRecord] {

      override def lens[K1, V1, K2, V2]: PLens[
        ProducerRecord[K1, V1],
        ProducerRecord[K2, V2],
        ProducerRecord[K1, V1],
        ProducerRecord[K2, V2]] =
        PLens[ProducerRecord[K1, V1], ProducerRecord[K2, V2], ProducerRecord[K1, V1], ProducerRecord[K2, V2]](
          s => s)(b => _ => b)
    }

  implicit val iprbi2: Aux[Fs2ProducerRecord] =
    new NJProducerMessage[Fs2ProducerRecord] {

      override def lens[K1, V1, K2, V2]: PLens[
        Fs2ProducerRecord[K1, V1],
        Fs2ProducerRecord[K2, V2],
        ProducerRecord[K1, V1],
        ProducerRecord[K2, V2]] =
        PLens[
          Fs2ProducerRecord[K1, V1],
          Fs2ProducerRecord[K2, V2],
          ProducerRecord[K1, V1],
          ProducerRecord[K2, V2]](_.transformInto)(b => _ => b.transformInto)
    }

  implicit def iprbi3[P]: Aux[AkkaProducerMessage[*, *, P]] =
    new NJProducerMessage[AkkaProducerMessage[*, *, P]] {

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
