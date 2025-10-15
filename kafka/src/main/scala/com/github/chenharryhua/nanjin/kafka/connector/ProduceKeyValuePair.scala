package com.github.chenharryhua.nanjin.kafka.connector

import cats.{Endo, Foldable}
import cats.effect.kernel.*
import cats.implicits.{catsSyntaxFlatten, toFoldableOps}
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.common.{HasProperties, UpdateConfig}
import fs2.kafka.*
import fs2.{Chunk, Pipe}
import org.apache.kafka.clients.producer.RecordMetadata

final class ProduceKeyValuePair[F[_], K, V] private[kafka] (
  topicName: TopicName,
  producerSettings: ProducerSettings[F, K, V])
    extends UpdateConfig[ProducerSettings[F, K, V], ProduceKeyValuePair[F, K, V]] with HasProperties {

  /*
   * config
   */
  override def properties: Map[String, String] = producerSettings.properties

  override def updateConfig(f: Endo[ProducerSettings[F, K, V]]): ProduceKeyValuePair[F, K, V] =
    new ProduceKeyValuePair[F, K, V](topicName, f(producerSettings))

  /*
   * sink
   */

  def sink(implicit F: Async[F]): Pipe[F, Chunk[(K, V)], ProducerResult[K, V]] =
    KafkaProducer
      .pipe[F, K, V](producerSettings)
      .compose(_.map(_.map { case (k, v) => ProducerRecord(topicName.name.value, k, v) }))

  /*
   * for testing and repl
   */
  def produce[G[_]: Foldable](kvs: G[(K, V)])(implicit F: Async[F]): F[ProducerResult[K, V]] = {
    val prs = Chunk.from(kvs.toList).map { case (k, v) => ProducerRecord(topicName.name.value, k, v) }
    KafkaProducer.resource(producerSettings).use(_.produce(prs).flatten)
  }

  def produceOne(k: K, v: V)(implicit F: Async[F]): F[RecordMetadata] =
    KafkaProducer.resource(producerSettings).use(_.produceOne_(topicName.name.value, k, v).flatten)
}
