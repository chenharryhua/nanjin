package com.github.chenharryhua.nanjin.codec

import akka.kafka.ConsumerMessage.{CommittableMessage => AkkaConsumerMessage}
import cats.implicits._
import cats.{Applicative, Bitraverse, Eval}
import fs2.kafka.{
  CommittableConsumerRecord => Fs2ConsumerMessage,
  ConsumerRecord            => Fs2ConsumerRecord
}
import monocle.PLens
import org.apache.kafka.clients.consumer.ConsumerRecord

sealed trait LikeConsumerRecord[F[_, _]] extends Bitraverse[F] with BitraverseKafkaRecord {

  def lens[K1, V1, K2, V2]
    : PLens[F[K1, V1], F[K2, V2], ConsumerRecord[K1, V1], ConsumerRecord[K2, V2]]

  final override def bimap[K1, V1, K2, V2](fkv: F[K1, V1])(k: K1 => K2, v: V1 => V2): F[K2, V2] =
    lens.modify((cr: ConsumerRecord[K1, V1]) => cr.bimap(k, v))(fkv)

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
    new LikeConsumerRecord[Fs2ConsumerRecord] with MessagePropertiesFs2 {
      override def lens[K1, V1, K2, V2]: PLens[
        Fs2ConsumerRecord[K1, V1],
        Fs2ConsumerRecord[K2, V2],
        ConsumerRecord[K1, V1],
        ConsumerRecord[K2, V2]] =
        PLens[
          Fs2ConsumerRecord[K1, V1],
          Fs2ConsumerRecord[K2, V2],
          ConsumerRecord[K1, V1],
          ConsumerRecord[K2, V2]](toConsumerRecord)(b => _ => fromConsumerRecord(b))
    }

  implicit val akkaConsumerMessageLike: LikeConsumerRecord[AkkaConsumerMessage] =
    new LikeConsumerRecord[AkkaConsumerMessage] {
      override def lens[K1, V1, K2, V2]: PLens[
        AkkaConsumerMessage[K1, V1],
        AkkaConsumerMessage[K2, V2],
        ConsumerRecord[K1, V1],
        ConsumerRecord[K2, V2]] =
        PLens[
          AkkaConsumerMessage[K1, V1],
          AkkaConsumerMessage[K2, V2],
          ConsumerRecord[K1, V1],
          ConsumerRecord[K2, V2]](_.record)(b => s => s.copy(record = b))
    }

  implicit def fs2ConsumerMessageLike[F[_]]: LikeConsumerRecord[Fs2ConsumerMessage[F, *, *]] =
    new LikeConsumerRecord[Fs2ConsumerMessage[F, *, *]] with MessagePropertiesFs2 {
      override def lens[K1, V1, K2, V2]: PLens[
        Fs2ConsumerMessage[F, K1, V1],
        Fs2ConsumerMessage[F, K2, V2],
        ConsumerRecord[K1, V1],
        ConsumerRecord[K2, V2]] =
        PLens[
          Fs2ConsumerMessage[F, K1, V1],
          Fs2ConsumerMessage[F, K2, V2],
          ConsumerRecord[K1, V1],
          ConsumerRecord[K2, V2]](cm => toConsumerRecord(cm.record))(b =>
          s                          => Fs2ConsumerMessage(fromConsumerRecord(b), s.offset))
    }
}
