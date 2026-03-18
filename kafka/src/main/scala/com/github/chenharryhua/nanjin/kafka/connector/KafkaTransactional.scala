package com.github.chenharryhua.nanjin.kafka.connector

import cats.Endo
import cats.effect.kernel.{Async, Resource}
import com.github.chenharryhua.nanjin.common.{HasProperties, UpdateConfig}
import fs2.Stream
import fs2.kafka.*

final class KafkaTransactional[F[_]: Async, K, V] private[kafka] (
  txnSettings: TransactionalProducerSettings[F, K, V])
    extends UpdateConfig[TransactionalProducerSettings[F, K, V], KafkaTransactional[F, K, V]]
    with HasProperties {

  /*
   * config
   */
  override lazy val properties: Map[String, String] = txnSettings.producerSettings.properties

  override def updateConfig(f: Endo[TransactionalProducerSettings[F, K, V]]): KafkaTransactional[F, K, V] =
    new KafkaTransactional[F, K, V](f(txnSettings))

  /*
   * produce
   */

  lazy val clientR: Resource[F, TransactionalKafkaProducer.WithoutOffsets[F, K, V]] =
    TransactionalKafkaProducer.resource(txnSettings)

  lazy val clientS: Stream[F, TransactionalKafkaProducer.WithoutOffsets[F, K, V]] =
    TransactionalKafkaProducer.stream(txnSettings)
}
