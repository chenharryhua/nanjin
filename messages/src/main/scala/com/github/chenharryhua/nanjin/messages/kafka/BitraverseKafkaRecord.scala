package com.github.chenharryhua.nanjin.messages.kafka

import cats.{Applicative, Bitraverse, Eval}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord

private[kafka] trait BitraverseKafkaRecord {

  implicit final val bitraverseConsumerRecord: Bitraverse[ConsumerRecord] =
    new Bitraverse[ConsumerRecord] {

      override def bimap[K1, V1, K2, V2](
        cr: ConsumerRecord[K1, V1])(k: K1 => K2, v: V1 => V2): ConsumerRecord[K2, V2] =
        new ConsumerRecord[K2, V2](
          cr.topic,
          cr.partition,
          cr.offset,
          cr.timestamp,
          cr.timestampType,
          cr.serializedKeySize,
          cr.serializedValueSize,
          k(cr.key),
          v(cr.value),
          cr.headers,
          cr.leaderEpoch
        )

      override def bitraverse[G[_], A, B, C, D](fab: ConsumerRecord[A, B])(f: A => G[C], g: B => G[D])(
        implicit G: Applicative[G]): G[ConsumerRecord[C, D]] =
        G.map2(f(fab.key), g(fab.value))((k, v) => bimap(fab)(_ => k, _ => v))

      override def bifoldLeft[A, B, C](fab: ConsumerRecord[A, B], c: C)(f: (C, A) => C, g: (C, B) => C): C =
        g(f(c, fab.key), fab.value)

      override def bifoldRight[A, B, C](fab: ConsumerRecord[A, B], c: Eval[C])(
        f: (A, Eval[C]) => Eval[C],
        g: (B, Eval[C]) => Eval[C]): Eval[C] = g(fab.value, f(fab.key, c))
    }

  implicit final val bitraverseProducerRecord: Bitraverse[ProducerRecord] =
    new Bitraverse[ProducerRecord] {

      override def bimap[K1, V1, K2, V2](
        pr: ProducerRecord[K1, V1])(k: K1 => K2, v: V1 => V2): ProducerRecord[K2, V2] =
        new ProducerRecord[K2, V2](pr.topic, pr.partition, pr.timestamp, k(pr.key), v(pr.value), pr.headers)

      override def bitraverse[G[_], A, B, C, D](fab: ProducerRecord[A, B])(f: A => G[C], g: B => G[D])(
        implicit G: Applicative[G]): G[ProducerRecord[C, D]] =
        G.map2(f(fab.key), g(fab.value))((k, v) => bimap(fab)(_ => k, _ => v))

      override def bifoldLeft[A, B, C](fab: ProducerRecord[A, B], c: C)(f: (C, A) => C, g: (C, B) => C): C =
        g(f(c, fab.key), fab.value)

      override def bifoldRight[A, B, C](fab: ProducerRecord[A, B], c: Eval[C])(
        f: (A, Eval[C]) => Eval[C],
        g: (B, Eval[C]) => Eval[C]): Eval[C] = g(fab.value, f(fab.key, c))
    }
}
