package com.github.chenharryhua.nanjin.kafka.connector

import cats.Endo
import cats.effect.kernel.*
import cats.implicits.catsSyntaxFlatten
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.common.{HasProperties, UpdateConfig}
import fs2.kafka.*
import fs2.{Chunk, Pipe}
import org.apache.kafka.clients.producer.RecordMetadata

/** Best Fs2 Kafka Lib [[https://fd4s.github.io/fs2-kafka/]]
  *
  * [[https://redpanda.com/guides/kafka-performance/kafka-performance-tuning]]
  */

final class ProduceKVKafka[F[_], K, V] private[kafka] (
  topicName: TopicName,
  producerSettings: ProducerSettings[F, K, V])
    extends UpdateConfig[ProducerSettings[F, K, V], ProduceKVKafka[F, K, V]] with HasProperties {

  /*
   * config
   */
  override def properties: Map[String, String] = producerSettings.properties

  override def updateConfig(f: Endo[ProducerSettings[F, K, V]]): ProduceKVKafka[F, K, V] =
    new ProduceKVKafka[F, K, V](topicName, f(producerSettings))

  /*
   * produce
   */

  def sink(implicit F: Async[F]): Pipe[F, (K, V), ProducerResult[K, V]] =
    KafkaProducer
      .pipe[F, K, V](producerSettings)
      .compose(_.chunks.map(_.map { case (k, v) => ProducerRecord(topicName.value, k, v) }))

  /*
   * for testing and repl
   */
  def produce(kvs: List[(K, V)])(implicit F: Async[F]): F[ProducerResult[K, V]] = {
    val prs = Chunk.from(kvs).map { case (k, v) => ProducerRecord(topicName.value, k, v) }
    KafkaProducer.resource(producerSettings).use(_.produce(prs).flatten)
  }

  def produceOne(k: K, v: V)(implicit F: Async[F]): F[RecordMetadata] =
    KafkaProducer.resource(producerSettings).use(_.produceOne_(topicName.value, k, v).flatten)
}
