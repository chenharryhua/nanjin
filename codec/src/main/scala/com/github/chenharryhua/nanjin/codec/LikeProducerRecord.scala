package com.github.chenharryhua.nanjin.codec

import akka.kafka.ProducerMessage.{Message => AkkaProducerMessage}
import cats.implicits._
import cats.{Applicative, Bitraverse, Eval}
import fs2.kafka.{ProducerRecord => Fs2ProducerRecord}
import monocle.PLens
import org.apache.kafka.clients.producer.ProducerRecord

sealed trait LikeProducerRecord[F[_, _]] extends Bitraverse[F] with BitraverseKafkaRecord {

  def lens[K1, V1, K2, V2]
    : PLens[F[K1, V1], F[K2, V2], ProducerRecord[K1, V1], ProducerRecord[K2, V2]]

  final override def bimap[K1, V1, K2, V2](pkv: F[K1, V1])(k: K1 => K2, v: V1 => V2): F[K2, V2] =
    lens.modify((pr: ProducerRecord[K1, V1]) => pr.bimap(k, v))(pkv)

  final override def bitraverse[G[_], A, B, C, D](fab: F[A, B])(f: A => G[C], g: B => G[D])(
    implicit G: Applicative[G]): G[F[C, D]] =
    lens.modifyF((pr: ProducerRecord[A, B]) => pr.bitraverse(f, g))(fab)

  final override def bifoldLeft[A, B, C](fab: F[A, B], c: C)(f: (C, A) => C, g: (C, B) => C): C =
    lens.get(fab).bifoldLeft(c)(f, g)

  final override def bifoldRight[A, B, C](fab: F[A, B], c: Eval[C])(
    f: (A, Eval[C]) => Eval[C],
    g: (B, Eval[C]) => Eval[C]): Eval[C] =
    lens.get(fab).bifoldRight(c)(f, g)
}

object LikeProducerRecord {
  def apply[F[_, _]](implicit ev: LikeProducerRecord[F]): LikeProducerRecord[F] = ev

  implicit val identityProducerRecordLike: LikeProducerRecord[ProducerRecord] =
    new LikeProducerRecord[ProducerRecord] {

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

  implicit def fs2ProducerRecordLike[P]: LikeProducerRecord[Fs2ProducerRecord] =
    new LikeProducerRecord[Fs2ProducerRecord] with MessagePropertiesFs2 {
      override def lens[K1, V1, K2, V2]: PLens[
        Fs2ProducerRecord[K1, V1],
        Fs2ProducerRecord[K2, V2],
        ProducerRecord[K1, V1],
        ProducerRecord[K2, V2]] =
        PLens[
          Fs2ProducerRecord[K1, V1],
          Fs2ProducerRecord[K2, V2],
          ProducerRecord[K1, V1],
          ProducerRecord[K2, V2]](toProducerRecord)(b => _ => fromProducerRecord(b))
    }

  implicit def akkaProducerMessageLike[P]: LikeProducerRecord[AkkaProducerMessage[*, *, P]] =
    new LikeProducerRecord[AkkaProducerMessage[*, *, P]] {
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
