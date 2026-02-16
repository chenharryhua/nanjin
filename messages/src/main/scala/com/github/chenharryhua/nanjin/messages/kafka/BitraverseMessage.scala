package com.github.chenharryhua.nanjin.messages.kafka

import cats.syntax.bifoldable.toBifoldableOps
import cats.syntax.bitraverse.catsSyntaxBitraverse
import cats.{Applicative, Bitraverse, Eval}
import com.github.chenharryhua.nanjin.messages.kafka.instances.*
import fs2.kafka.{CommittableConsumerRecord, ConsumerRecord, ProducerRecord}
import io.scalaland.chimney.dsl.*
import monocle.PLens
import org.apache.kafka.clients.consumer.ConsumerRecord as JavaConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord as JavaProducerRecord

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
  final override type H[K, V] = JavaConsumerRecord[K, V]
  final override val baseInst: Bitraverse[JavaConsumerRecord] = bitraverseConsumerRecord
}

object NJConsumerMessage {
  final type Aux[F[_, _]] = NJConsumerMessage[F] { type H[K, V] = JavaConsumerRecord[K, V] }

  def apply[F[_, _]](implicit ev: NJConsumerMessage[F]): Aux[F] = ev

  implicit val njConsumerMessageJavaConsumerRecord: Aux[JavaConsumerRecord] =
    new NJConsumerMessage[JavaConsumerRecord] {

      override def lens[K1, V1, K2, V2]: PLens[
        JavaConsumerRecord[K1, V1],
        JavaConsumerRecord[K2, V2],
        JavaConsumerRecord[K1, V1],
        JavaConsumerRecord[K2, V2]] =
        PLens[
          JavaConsumerRecord[K1, V1],
          JavaConsumerRecord[K2, V2],
          JavaConsumerRecord[K1, V1],
          JavaConsumerRecord[K2, V2]](s => s)(b => _ => b)
    }

  implicit val njConsumerMessageConsumerRecord: Aux[ConsumerRecord] =
    new NJConsumerMessage[ConsumerRecord] {

      override def lens[K1, V1, K2, V2]: PLens[
        ConsumerRecord[K1, V1],
        ConsumerRecord[K2, V2],
        JavaConsumerRecord[K1, V1],
        JavaConsumerRecord[K2, V2]] =
        PLens[
          ConsumerRecord[K1, V1],
          ConsumerRecord[K2, V2],
          JavaConsumerRecord[K1, V1],
          JavaConsumerRecord[K2, V2]](_.transformInto[JavaConsumerRecord[K1, V1]])(b =>
          _ => b.transformInto[ConsumerRecord[K2, V2]])

    }

  implicit def njConsumerMessageCommittableConsumerRecord[F[_]]: Aux[CommittableConsumerRecord[F, *, *]] =
    new NJConsumerMessage[CommittableConsumerRecord[F, *, *]] {

      override def lens[K1, V1, K2, V2]: PLens[
        CommittableConsumerRecord[F, K1, V1],
        CommittableConsumerRecord[F, K2, V2],
        JavaConsumerRecord[K1, V1],
        JavaConsumerRecord[K2, V2]] =
        PLens[
          CommittableConsumerRecord[F, K1, V1],
          CommittableConsumerRecord[F, K2, V2],
          JavaConsumerRecord[K1, V1],
          JavaConsumerRecord[K2, V2]](_.record.transformInto[JavaConsumerRecord[K1, V1]]) { b => s =>
          CommittableConsumerRecord(b.transformInto[ConsumerRecord[K2, V2]], s.offset)
        }
    }

  implicit val njConsumerMessageNJConsumerRecord: Aux[NJConsumerRecord] =
    new NJConsumerMessage[NJConsumerRecord] {

      override def lens[K1, V1, K2, V2]: PLens[
        NJConsumerRecord[K1, V1],
        NJConsumerRecord[K2, V2],
        JavaConsumerRecord[K1, V1],
        JavaConsumerRecord[K2, V2]] =
        PLens[
          NJConsumerRecord[K1, V1],
          NJConsumerRecord[K2, V2],
          JavaConsumerRecord[K1, V1],
          JavaConsumerRecord[K2, V2]](_.transformInto[JavaConsumerRecord[K1, V1]])(b =>
          _ => b.transformInto[NJConsumerRecord[K2, V2]])
    }
}

sealed trait NJProducerMessage[F[_, _]] extends BitraverseMessage[F] with BitraverseKafkaRecord {
  final override type H[K, V] = JavaProducerRecord[K, V]
  final override val baseInst: Bitraverse[JavaProducerRecord] = bitraverseProducerRecord
}

object NJProducerMessage {
  final type Aux[F[_, _]] = NJProducerMessage[F] { type H[K, V] = JavaProducerRecord[K, V] }

  def apply[F[_, _]](implicit ev: NJProducerMessage[F]): Aux[F] = ev

  implicit val njProducerMessageJavaProducerRecord: Aux[JavaProducerRecord] =
    new NJProducerMessage[JavaProducerRecord] {

      override def lens[K1, V1, K2, V2]: PLens[
        JavaProducerRecord[K1, V1],
        JavaProducerRecord[K2, V2],
        JavaProducerRecord[K1, V1],
        JavaProducerRecord[K2, V2]] =
        PLens[
          JavaProducerRecord[K1, V1],
          JavaProducerRecord[K2, V2],
          JavaProducerRecord[K1, V1],
          JavaProducerRecord[K2, V2]](s => s)(b => _ => b)
    }

  implicit val njProducerMessageProducerRecord: Aux[ProducerRecord] =
    new NJProducerMessage[ProducerRecord] {

      override def lens[K1, V1, K2, V2]: PLens[
        ProducerRecord[K1, V1],
        ProducerRecord[K2, V2],
        JavaProducerRecord[K1, V1],
        JavaProducerRecord[K2, V2]] =
        PLens[
          ProducerRecord[K1, V1],
          ProducerRecord[K2, V2],
          JavaProducerRecord[K1, V1],
          JavaProducerRecord[K2, V2]](_.transformInto)(b => _ => b.transformInto)
    }

}
