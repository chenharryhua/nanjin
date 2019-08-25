package com.github.chenharryhua.nanjin.sparkafka

import cats.{Applicative, Bitraverse, Eval}
import monocle.macros.Lenses
import org.apache.kafka.clients.consumer.ConsumerRecord

//https://spark.apache.org/docs/2.4.3/structured-streaming-kafka-integration.html
@Lenses final case class SparkConsumerRecord[K, V](
  key: K,
  value: V,
  topic: String,
  partition: Int,
  offset: Long,
  timestamp: Long,
  timestampType: Int)

object SparkConsumerRecord {

  def from[K, V](cr: ConsumerRecord[K, V]) =
    SparkConsumerRecord(
      cr.key(),
      cr.value(),
      cr.topic(),
      cr.partition(),
      cr.offset(),
      cr.timestamp(),
      cr.timestampType().id
    )

  implicit def bitraverseSparkConsumerRecord[K, V]: Bitraverse[SparkConsumerRecord] =
    new Bitraverse[SparkConsumerRecord] {
      override def bimap[A, B, C, D](
        fab: SparkConsumerRecord[A, B])(f: A => C, g: B => D): SparkConsumerRecord[C, D] =
        fab.copy(key = f(fab.key), value = g(fab.value))

      override def bitraverse[G[_], A, B, C, D](fab: SparkConsumerRecord[A, B])(
        f: A => G[C],
        g: B => G[D])(implicit G: Applicative[G]): G[SparkConsumerRecord[C, D]] =
        G.map2(f(fab.key), g(fab.value))((k, v) => bimap(fab)(_ => k, _ => v))

      override def bifoldLeft[A, B, C](fab: SparkConsumerRecord[A, B], c: C)(
        f: (C, A) => C,
        g: (C, B) => C): C = g(f(c, fab.key), fab.value)

      override def bifoldRight[A, B, C](fab: SparkConsumerRecord[A, B], c: Eval[C])(
        f: (A, Eval[C]) => Eval[C],
        g: (B, Eval[C]) => Eval[C]): Eval[C] = g(fab.value, f(fab.key, c))
    }
}
