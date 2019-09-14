package com.github.chenharryhua.nanjin.codec

import akka.kafka.ProducerMessage.{MultiMessage => AkkaMultiMessage}
import cats.implicits._
import cats.{Applicative, Bitraverse, Eval}
import fs2.Chunk
import fs2.kafka.{
  CommittableProducerRecords   => Fs2CommittableProducerRecords,
  ProducerRecords              => Fs2ProducerRecords,
  TransactionalProducerRecords => Fs2TransactionalProducerRecords
}
import monocle.{PLens, PTraversal}
import org.apache.kafka.clients.producer.ProducerRecord

sealed trait LikeProducerRecords[F[_, _]] extends Bitraverse[F] {

  def traversal[K1, V1, K2, V2]
    : PTraversal[F[K1, V1], F[K2, V2], ProducerRecord[K1, V1], ProducerRecord[K2, V2]]

  final override def bitraverse[G[_], A, B, C, D](fab: F[A, B])(f: A => G[C], g: B => G[D])(
    implicit G: Applicative[G]): G[F[C, D]] =
    traversal.modifyF((pr: ProducerRecord[A, B]) => pr.bitraverse(f, g))(fab)

  final override def bifoldLeft[A, B, C](fab: F[A, B], c: C)(f: (C, A) => C, g: (C, B) => C): C =
    traversal.getAll(fab).foldLeft(c) { case (cp, rec) => rec.bifoldLeft(cp)(f, g) }

  final override def bifoldRight[A, B, C](fab: F[A, B], c: Eval[C])(
    f: (A, Eval[C]) => Eval[C],
    g: (B, Eval[C]) => Eval[C]): Eval[C] =
    traversal.getAll(fab).foldRight(c) { case (rec, cp) => rec.bifoldRight(cp)(f, g) }
}

object LikeProducerRecords {
  def apply[F[_, _]](implicit ev: LikeProducerRecords[F]): LikeProducerRecords[F] = ev

  implicit def fs2ProducerRecordsLike[P]: LikeProducerRecords[Fs2ProducerRecords[*, *, P]] =
    new LikeProducerRecords[Fs2ProducerRecords[*, *, P]] {
      override def traversal[K1, V1, K2, V2]: PTraversal[
        Fs2ProducerRecords[K1, V1, P],
        Fs2ProducerRecords[K2, V2, P],
        ProducerRecord[K1, V1],
        ProducerRecord[K2, V2]] =
        PLens[
          Fs2ProducerRecords[K1, V1, P],
          Fs2ProducerRecords[K2, V2, P],
          Chunk[ProducerRecord[K1, V1]],
          Chunk[ProducerRecord[K2, V2]]](prs => prs.records.map(r => isoFs2ProducerRecord.get(r))) {
          cpr => s =>
            Fs2ProducerRecords(cpr.map(isoFs2ProducerRecord.reverseGet), s.passthrough)
        }.composeTraversal(
          PTraversal.fromTraverse[Chunk, ProducerRecord[K1, V1], ProducerRecord[K2, V2]])
    }

  implicit def fs2CommittableProducerRecordsLike[F[_]]
    : LikeProducerRecords[Fs2CommittableProducerRecords[F, *, *]] =
    new LikeProducerRecords[Fs2CommittableProducerRecords[F, *, *]] {
      override def traversal[K1, V1, K2, V2]: PTraversal[
        Fs2CommittableProducerRecords[F, K1, V1],
        Fs2CommittableProducerRecords[F, K2, V2],
        ProducerRecord[K1, V1],
        ProducerRecord[K2, V2]] =
        PLens[
          Fs2CommittableProducerRecords[F, K1, V1],
          Fs2CommittableProducerRecords[F, K2, V2],
          Chunk[ProducerRecord[K1, V1]],
          Chunk[ProducerRecord[K2, V2]]](prs => prs.records.map(r => isoFs2ProducerRecord.get(r))) {
          cpr => s =>
            Fs2CommittableProducerRecords(cpr.map(isoFs2ProducerRecord.reverseGet), s.offset)
        }.composeTraversal(
          PTraversal.fromTraverse[Chunk, ProducerRecord[K1, V1], ProducerRecord[K2, V2]])
    }

  implicit def fs2TransactionalProducerRecordsLike[F[_], P]
    : LikeProducerRecords[Fs2TransactionalProducerRecords[F, *, *, P]] =
    new LikeProducerRecords[Fs2TransactionalProducerRecords[F, *, *, P]] {
      override def traversal[K1, V1, K2, V2]: PTraversal[
        Fs2TransactionalProducerRecords[F, K1, V1, P],
        Fs2TransactionalProducerRecords[F, K2, V2, P],
        ProducerRecord[K1, V1],
        ProducerRecord[K2, V2]] =
        PLens[
          Fs2TransactionalProducerRecords[F, K1, V1, P],
          Fs2TransactionalProducerRecords[F, K2, V2, P],
          Chunk[Fs2CommittableProducerRecords[F, K1, V1]],
          Chunk[Fs2CommittableProducerRecords[F, K2, V2]]](_.records)(b =>
          s => Fs2TransactionalProducerRecords(b, s.passthrough))
          .composeTraversal(
            PTraversal.fromTraverse[
              Chunk,
              Fs2CommittableProducerRecords[F, K1, V1],
              Fs2CommittableProducerRecords[F, K2, V2]])
          .composeTraversal(fs2CommittableProducerRecordsLike.traversal)
    }

  implicit def akkaMultiMessageLike[P]: LikeProducerRecords[AkkaMultiMessage[*, *, P]] =
    new LikeProducerRecords[AkkaMultiMessage[*, *, P]] {
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
        }.composeTraversal(
          PTraversal.fromTraverse[Chunk, ProducerRecord[K1, V1], ProducerRecord[K2, V2]])
    }
}
