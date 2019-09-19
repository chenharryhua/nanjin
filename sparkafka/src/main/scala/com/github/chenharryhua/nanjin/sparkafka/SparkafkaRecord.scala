package com.github.chenharryhua.nanjin.sparkafka

import cats.{Applicative, Bitraverse, Eval}
import monocle.macros.Lenses
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord

// https://spark.apache.org/docs/2.4.3/structured-streaming-kafka-integration.html
@Lenses final case class SparkafkaConsumerRecord[K, V](
  key: K,
  value: V,
  topic: String,
  partition: Int,
  offset: Long,
  timestamp: Long,
  timestampType: Int) {

  def toSparkafkaProducerRecord: SparkafkaProducerRecord[K, V] =
    SparkafkaProducerRecord[K, V](topic, Option(partition), Option(timestamp), Option(key), value)
}

object SparkafkaConsumerRecord {

  def from[K, V](cr: ConsumerRecord[K, V]) =
    SparkafkaConsumerRecord(
      cr.key(),
      cr.value(),
      cr.topic(),
      cr.partition(),
      cr.offset(),
      cr.timestamp(),
      cr.timestampType().id)

  implicit val bitraverseSparkafkaConsumerRecord: Bitraverse[SparkafkaConsumerRecord] =
    new Bitraverse[SparkafkaConsumerRecord] {
      override def bimap[A, B, C, D](
        fab: SparkafkaConsumerRecord[A, B])(f: A => C, g: B => D): SparkafkaConsumerRecord[C, D] =
        fab.copy(key = f(fab.key), value = g(fab.value))

      override def bitraverse[G[_], A, B, C, D](fab: SparkafkaConsumerRecord[A, B])(
        f: A => G[C],
        g: B => G[D])(implicit G: Applicative[G]): G[SparkafkaConsumerRecord[C, D]] =
        G.map2(f(fab.key), g(fab.value))((k, v) => bimap(fab)(_ => k, _ => v))

      override def bifoldLeft[A, B, C](fab: SparkafkaConsumerRecord[A, B], c: C)(
        f: (C, A) => C,
        g: (C, B) => C): C = g(f(c, fab.key), fab.value)

      override def bifoldRight[A, B, C](fab: SparkafkaConsumerRecord[A, B], c: Eval[C])(
        f: (A, Eval[C]) => Eval[C],
        g: (B, Eval[C]) => Eval[C]): Eval[C] = g(fab.value, f(fab.key, c))
    }
}

@Lenses final case class SparkafkaProducerRecord[K, V](
  topic: String,
  partition: Option[Int],
  timestamp: Option[Long],
  key: Option[K],
  value: V) {

  @SuppressWarnings(Array("AsInstanceOf"))
  def toProducerRecord: ProducerRecord[K, V] = new ProducerRecord[K, V](
    topic,
    partition.getOrElse(null.asInstanceOf[Int]),
    timestamp.getOrElse(null.asInstanceOf[Long]),
    key.getOrElse(null.asInstanceOf[K]),
    value
  )
}

object SparkafkaProducerRecord {

  def apply[K, V](topic: String, k: K, v: V): SparkafkaProducerRecord[K, V] =
    SparkafkaProducerRecord(topic, None, None, Option(k), v)

  def apply[K, V](topic: String, v: V): SparkafkaProducerRecord[K, V] =
    SparkafkaProducerRecord(topic, None, None, None, v)

}
