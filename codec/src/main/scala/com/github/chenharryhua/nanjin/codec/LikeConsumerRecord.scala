package com.github.chenharryhua.nanjin.codec

import akka.kafka.ConsumerMessage.{CommittableMessage => AkkaCommittableMessage}
import cats.implicits._
import cats.{Applicative, Bitraverse, Eval}
import fs2.kafka.{
  CommittableConsumerRecord => Fs2CommittableConsumerRecord,
  ConsumerRecord            => Fs2ConsumerRecord
}
import monocle.PLens
import org.apache.kafka.clients.consumer.ConsumerRecord

sealed trait LikeConsumerRecord[F[_, _]] extends Bitraverse[F] {

  def lens[K1, V1, K2, V2]
    : PLens[F[K1, V1], F[K2, V2], ConsumerRecord[K1, V1], ConsumerRecord[K2, V2]]

  final override def bitraverse[G[_], A, B, C, D](fab: F[A, B])(f: A => G[C], g: B => G[D])(
    implicit G: Applicative[G]): G[F[C, D]] =
    lens.modifyF((cr: ConsumerRecord[A, B]) => cr.bitraverse(f, g))(fab)

  final override def bifoldLeft[A, B, C](fab: F[A, B], c: C)(f: (C, A) => C, g: (C, B) => C): C =
    lens.get(fab).bifoldLeft(c)(f, g)

  final override def bifoldRight[A, B, C](fab: F[A, B], c: Eval[C])(
    f: (A, Eval[C]) => Eval[C],
    g: (B, Eval[C]) => Eval[C]): Eval[C] =
    lens.get(fab).bifoldRight(c)(f, g)
}

object LikeConsumerRecord {
  def apply[F[_, _]](implicit ev: LikeConsumerRecord[F]): LikeConsumerRecord[F] = ev

  implicit val identityConsumerRecordLike: LikeConsumerRecord[ConsumerRecord] =
    new LikeConsumerRecord[ConsumerRecord] {
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
  implicit val fs2ConsumerRecordLike: LikeConsumerRecord[Fs2ConsumerRecord] =
    new LikeConsumerRecord[Fs2ConsumerRecord] {
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

  implicit val akkaConsumerMessageLike: LikeConsumerRecord[AkkaCommittableMessage] =
    new LikeConsumerRecord[AkkaCommittableMessage] {
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

  implicit def fs2ConsumerMessageLike[F[_]]
    : LikeConsumerRecord[Fs2CommittableConsumerRecord[F, *, *]] =
    new LikeConsumerRecord[Fs2CommittableConsumerRecord[F, *, *]] {
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
}
