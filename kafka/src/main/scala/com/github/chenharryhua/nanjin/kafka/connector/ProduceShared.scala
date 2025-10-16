package com.github.chenharryhua.nanjin.kafka.connector

import cats.Endo
import cats.effect.kernel.{Async, Resource}
import com.github.chenharryhua.nanjin.common.{HasProperties, UpdateConfig}
import fs2.{Pipe, Stream}
import fs2.kafka.{
  KafkaProducer,
  ProducerRecords,
  ProducerResult,
  ProducerSettings,
  TransactionalKafkaProducer,
  TransactionalProducerSettings
}

final class ProduceShared[F[_], K, V] private[kafka](producerSettings: ProducerSettings[F, K, V])
    extends UpdateConfig[ProducerSettings[F, K, V], ProduceShared[F, K, V]] with HasProperties {
  override def updateConfig(f: Endo[ProducerSettings[F, K, V]]): ProduceShared[F, K, V] =
    new ProduceShared[F, K, V](f(producerSettings))

  override def properties: Map[String, String] = producerSettings.properties

  def clientR(implicit F: Async[F]): Resource[F, KafkaProducer.Metrics[F, K, V]] =
    KafkaProducer.resource(producerSettings)

  def clientS(implicit F: Async[F]): Stream[F, KafkaProducer.Metrics[F, K, V]] =
    KafkaProducer.stream(producerSettings)

  def transactional(transactionalId: String): KafkaTransactional[F, K, V] =
    new KafkaTransactional[F, K, V](TransactionalProducerSettings(transactionalId, producerSettings))

  def sink(implicit F: Async[F]): Pipe[F, ProducerRecords[K, V], ProducerResult[K, V]] =
    KafkaProducer.pipe[F, K, V](producerSettings)
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
