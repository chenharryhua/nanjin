package com.github.chenharryhua.nanjin.kafka.connector

import cats.Endo
import cats.effect.kernel.*
import cats.implicits.catsSyntaxFlatten
import com.github.chenharryhua.nanjin.common.{HasProperties, UpdateConfig}
import fs2.kafka.*
import fs2.{Pipe, Stream}
import org.apache.kafka.clients.producer.RecordMetadata

/** Best Fs2 Kafka Lib [[https://fd4s.github.io/fs2-kafka/]]
  *
  * [[https://redpanda.com/guides/kafka-performance/kafka-performance-tuning]]
  */

final class KafkaProduce[F[_], K, V] private[kafka] (producerSettings: ProducerSettings[F, K, V])
    extends UpdateConfig[ProducerSettings[F, K, V], KafkaProduce[F, K, V]] with HasProperties {

  /*
   * config
   */
  override def properties: Map[String, String] = producerSettings.properties

  override def updateConfig(f: Endo[ProducerSettings[F, K, V]]): KafkaProduce[F, K, V] =
    new KafkaProduce[F, K, V](f(producerSettings))

  /*
   * produce
   */

  def transactional(transactionalId: String): KafkaTransactional[F, K, V] =
    new KafkaTransactional[F, K, V](TransactionalProducerSettings(transactionalId, producerSettings))

  def clientR(implicit F: Async[F]): Resource[F, KafkaProducer.Metrics[F, K, V]] =
    KafkaProducer.resource(producerSettings)

  def sink(implicit F: Async[F]): Pipe[F, ProducerRecords[K, V], ProducerResult[K, V]] =
    KafkaProducer.pipe[F, K, V](producerSettings)

  def clientS(implicit F: Async[F]): Stream[F, KafkaProducer.Metrics[F, K, V]] =
    KafkaProducer.stream(producerSettings)

  /*
   * for testing and repl
   */
  def produceOne(pr: ProducerRecord[K, V])(implicit F: Async[F]): F[RecordMetadata] =
    clientR.use(_.produceOne_(pr).flatten)

  def produceOne(topicName: String, k: K, v: V)(implicit F: Async[F]): F[RecordMetadata] =
    produceOne(ProducerRecord(topicName, k, v))

}

final class KafkaTransactional[F[_], K, V] private[kafka] (
  txnSettings: TransactionalProducerSettings[F, K, V])
    extends UpdateConfig[TransactionalProducerSettings[F, K, V], KafkaTransactional[F, K, V]]
    with HasProperties {

  /*
   * config
   */
  override def properties: Map[String, String] = txnSettings.producerSettings.properties

  override def updateConfig(f: Endo[TransactionalProducerSettings[F, K, V]]): KafkaTransactional[F, K, V] =
    new KafkaTransactional[F, K, V](f(txnSettings))

  /*
   * produce
   */

  def clientR(implicit F: Async[F]): Resource[F, TransactionalKafkaProducer.WithoutOffsets[F, K, V]] =
    TransactionalKafkaProducer.resource(txnSettings)

  def clientS(implicit F: Async[F]): Stream[F, TransactionalKafkaProducer.WithoutOffsets[F, K, V]] =
    TransactionalKafkaProducer.stream(txnSettings)
}
