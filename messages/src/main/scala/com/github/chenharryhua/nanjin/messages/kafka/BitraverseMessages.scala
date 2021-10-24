package com.github.chenharryhua.nanjin.messages.kafka

import akka.kafka.ProducerMessage.MultiMessage as AkkaMultiMessage
import cats.syntax.all.*
import cats.{Applicative, Bitraverse, Eval}
import com.github.chenharryhua.nanjin.messages.kafka.instances.*
import fs2.Chunk
import fs2.kafka.{
  CommittableProducerRecords as Fs2CommittableProducerRecords,
  ProducerRecords as Fs2ProducerRecords,
  TransactionalProducerRecords as Fs2TransactionalProducerRecords
}
import io.scalaland.chimney.dsl.*
import monocle.{PLens, PTraversal}
import org.apache.kafka.clients.producer.ProducerRecord

sealed trait BitraverseMessages[F[_, _]] extends Bitraverse[F] with BitraverseKafkaRecord {

  def traversal[K1, V1, K2, V2]: PTraversal[F[K1, V1], F[K2, V2], ProducerRecord[K1, V1], ProducerRecord[K2, V2]]

  final override def bitraverse[G[_], A, B, C, D](fab: F[A, B])(f: A => G[C], g: B => G[D])(implicit
    G: Applicative[G]): G[F[C, D]] =
    traversal.modifyA((pr: ProducerRecord[A, B]) => pr.bitraverse(f, g))(fab)

  final override def bifoldLeft[A, B, C](fab: F[A, B], c: C)(f: (C, A) => C, g: (C, B) => C): C =
    traversal.getAll(fab).foldLeft(c) { case (cp, rec) => rec.bifoldLeft(cp)(f, g) }

  final override def bifoldRight[A, B, C](fab: F[A, B], c: Eval[C])(
    f: (A, Eval[C]) => Eval[C],
    g: (B, Eval[C]) => Eval[C]): Eval[C] =
    traversal.getAll(fab).foldRight(c) { case (rec, cp) => rec.bifoldRight(cp)(f, g) }
}

object BitraverseMessages {
  def apply[F[_, _]](implicit ev: BitraverseMessages[F]): BitraverseMessages[F] = ev

  implicit def imsbi1[P]: BitraverseMessages[Fs2ProducerRecords[P, *, *]] =
    new BitraverseMessages[Fs2ProducerRecords[P, *, *]] {

      override def traversal[K1, V1, K2, V2]: PTraversal[
        Fs2ProducerRecords[P, K1, V1],
        Fs2ProducerRecords[P, K2, V2],
        ProducerRecord[K1, V1],
        ProducerRecord[K2, V2]] =
        PLens[
          Fs2ProducerRecords[P, K1, V1],
          Fs2ProducerRecords[P, K2, V2],
          Chunk[ProducerRecord[K1, V1]],
          Chunk[ProducerRecord[K2, V2]]](prs => prs.records.map(_.transformInto)) { cpr => s =>
          Fs2ProducerRecords(cpr.map(_.transformInto), s.passthrough)
        }.andThen(PTraversal.fromTraverse[Chunk, ProducerRecord[K1, V1], ProducerRecord[K2, V2]])
    }

  implicit def imsbi2[F[_]]: BitraverseMessages[Fs2CommittableProducerRecords[F, *, *]] =
    new BitraverseMessages[Fs2CommittableProducerRecords[F, *, *]] {

      override def traversal[K1, V1, K2, V2]: PTraversal[
        Fs2CommittableProducerRecords[F, K1, V1],
        Fs2CommittableProducerRecords[F, K2, V2],
        ProducerRecord[K1, V1],
        ProducerRecord[K2, V2]] =
        PLens[
          Fs2CommittableProducerRecords[F, K1, V1],
          Fs2CommittableProducerRecords[F, K2, V2],
          Chunk[ProducerRecord[K1, V1]],
          Chunk[ProducerRecord[K2, V2]]](prs => prs.records.map(_.transformInto)) { cpr => s =>
          Fs2CommittableProducerRecords(cpr.map(_.transformInto), s.offset)
        }.andThen(PTraversal.fromTraverse[Chunk, ProducerRecord[K1, V1], ProducerRecord[K2, V2]])
    }

  implicit def imsbi3[F[_], P]: BitraverseMessages[Fs2TransactionalProducerRecords[F, P, *, *]] =
    new BitraverseMessages[Fs2TransactionalProducerRecords[F, P, *, *]] {

      override def traversal[K1, V1, K2, V2]: PTraversal[
        Fs2TransactionalProducerRecords[F, P, K1, V1],
        Fs2TransactionalProducerRecords[F, P, K2, V2],
        ProducerRecord[K1, V1],
        ProducerRecord[K2, V2]] =
        PLens[
          Fs2TransactionalProducerRecords[F, P, K1, V1],
          Fs2TransactionalProducerRecords[F, P, K2, V2],
          Chunk[Fs2CommittableProducerRecords[F, K1, V1]],
          Chunk[Fs2CommittableProducerRecords[F, K2, V2]]](_.records)(b =>
          s => Fs2TransactionalProducerRecords(b, s.passthrough))
          .andThen(PTraversal
            .fromTraverse[Chunk, Fs2CommittableProducerRecords[F, K1, V1], Fs2CommittableProducerRecords[F, K2, V2]])
          .andThen(imsbi2.traversal)
    }

  implicit def imsbi4[P]: BitraverseMessages[AkkaMultiMessage[*, *, P]] =
    new BitraverseMessages[AkkaMultiMessage[*, *, P]] {

      override def traversal[K1, V1, K2, V2]: PTraversal[
        AkkaMultiMessage[K1, V1, P],
        AkkaMultiMessage[K2, V2, P],
        ProducerRecord[K1, V1],
        ProducerRecord[K2, V2]] =
        PLens[
          AkkaMultiMessage[K1, V1, P],
          AkkaMultiMessage[K2, V2, P],
          Chunk[ProducerRecord[K1, V1]],
          Chunk[ProducerRecord[K2, V2]]](prs => Chunk.seq(prs.records)) { b => s =>
          s.copy(records = b.toList)
        }.andThen(PTraversal.fromTraverse[Chunk, ProducerRecord[K1, V1], ProducerRecord[K2, V2]])
    }
}
