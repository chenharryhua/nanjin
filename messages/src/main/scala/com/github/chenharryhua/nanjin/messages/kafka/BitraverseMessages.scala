package com.github.chenharryhua.nanjin.messages.kafka

import cats.syntax.bifoldable.toBifoldableOps
import cats.syntax.bitraverse.catsSyntaxBitraverse
import cats.{Applicative, Bitraverse, Eval}
import com.github.chenharryhua.nanjin.messages.kafka.instances.*
import fs2.Chunk
import fs2.kafka.{CommittableProducerRecords, ProducerRecords, TransactionalProducerRecords}
import io.scalaland.chimney.dsl.*
import monocle.{PLens, PTraversal}
import org.apache.kafka.clients.producer.ProducerRecord

sealed trait BitraverseMessages[F[_, _]] extends Bitraverse[F] with BitraverseKafkaRecord {

  def traversal[K1, V1, K2, V2]
    : PTraversal[F[K1, V1], F[K2, V2], ProducerRecord[K1, V1], ProducerRecord[K2, V2]]

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

  implicit def bitraverseMessagesCommittableProducerRecords[F[_]]
    : BitraverseMessages[CommittableProducerRecords[F, *, *]] =
    new BitraverseMessages[CommittableProducerRecords[F, *, *]] {

      override def traversal[K1, V1, K2, V2]: PTraversal[
        CommittableProducerRecords[F, K1, V1],
        CommittableProducerRecords[F, K2, V2],
        ProducerRecord[K1, V1],
        ProducerRecord[K2, V2]] =
        PLens[
          CommittableProducerRecords[F, K1, V1],
          CommittableProducerRecords[F, K2, V2],
          Chunk[ProducerRecord[K1, V1]],
          Chunk[ProducerRecord[K2, V2]]](prs => prs.records.map(_.transformInto)) { cpr => s =>
          CommittableProducerRecords(cpr.map(_.transformInto), s.offset)
        }.andThen(PTraversal.fromTraverse[Chunk, ProducerRecord[K1, V1], ProducerRecord[K2, V2]])
    }

  implicit val bitraverseMessagesProducerRecords: BitraverseMessages[ProducerRecords[*, *]] =
    new BitraverseMessages[ProducerRecords[*, *]] {

      override def traversal[K1, V1, K2, V2]: PTraversal[
        ProducerRecords[K1, V1],
        ProducerRecords[K2, V2],
        ProducerRecord[K1, V1],
        ProducerRecord[K2, V2]] =
        PLens[
          ProducerRecords[K1, V1],
          ProducerRecords[K2, V2],
          Chunk[ProducerRecord[K1, V1]],
          Chunk[ProducerRecord[K2, V2]]](prs => prs.map(_.transformInto)) { cpr => _ =>
          cpr.map(_.transformInto)
        }.andThen(PTraversal.fromTraverse[Chunk, ProducerRecord[K1, V1], ProducerRecord[K2, V2]])
    }

  implicit def bitraverseMessagesTransactionalProducerRecords[F[_]]
    : BitraverseMessages[TransactionalProducerRecords[F, *, *]] =
    new BitraverseMessages[TransactionalProducerRecords[F, *, *]] {

      override def traversal[K1, V1, K2, V2]: PTraversal[
        TransactionalProducerRecords[F, K1, V1],
        TransactionalProducerRecords[F, K2, V2],
        ProducerRecord[K1, V1],
        ProducerRecord[K2, V2]] =
        PLens[
          TransactionalProducerRecords[F, K1, V1],
          TransactionalProducerRecords[F, K2, V2],
          Chunk[ProducerRecord[K1, V1]],
          Chunk[ProducerRecord[K2, V2]]
        ](prs => prs.flatMap(_.records.map(_.transformInto))) { cpr => s =>
          s.last.map(_.offset) match {
            case Some(value) => Chunk.singleton(CommittableProducerRecords(cpr.map(_.transformInto), value))
            case None        => Chunk.empty
          }
        }.andThen(PTraversal.fromTraverse[Chunk, ProducerRecord[K1, V1], ProducerRecord[K2, V2]])
    }

}
