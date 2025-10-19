package com.github.chenharryhua.nanjin.kafka.connector

import cats.Endo
import cats.effect.kernel.{Async, Resource}
import cats.implicits.toFunctorOps
import com.github.chenharryhua.nanjin.common.{HasProperties, UpdateConfig}
import fs2.kafka.*
import fs2.{Chunk, Pipe, Stream}
import org.apache.kafka.clients.producer.RecordMetadata

/*
 * Shared Producer
 */
final class ProduceShared[F[_]: Async, K, V] private[kafka] (producerSettings: ProducerSettings[F, K, V])
    extends UpdateConfig[ProducerSettings[F, K, V], ProduceShared[F, K, V]] with HasProperties {
  override def updateConfig(f: Endo[ProducerSettings[F, K, V]]): ProduceShared[F, K, V] =
    new ProduceShared[F, K, V](f(producerSettings))

  override def properties: Map[String, String] = producerSettings.properties

  lazy val clientR: Resource[F, KafkaProducer.Metrics[F, K, V]] =
    KafkaProducer.resource(producerSettings)

  lazy val clientS: Stream[F, KafkaProducer.Metrics[F, K, V]] =
    KafkaProducer.stream(producerSettings)

  def transactional(transactionalId: String): KafkaTransactional[F, K, V] =
    new KafkaTransactional[F, K, V](TransactionalProducerSettings(transactionalId, producerSettings))

  lazy val sink: Pipe[F, ProducerRecord[K, V], Chunk[RecordMetadata]] =
    (ss: Stream[F, ProducerRecord[K, V]]) =>
      KafkaProducer.stream[F, K, V](producerSettings).flatMap { producer =>
        ss.chunks.evalMap(producer.produce).parEvalMap(Int.MaxValue)(_.map(_.map(_._2)))
      }
}

final class KafkaTransactional[F[_]: Async, K, V] private[kafka] (
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

  lazy val clientR: Resource[F, TransactionalKafkaProducer.WithoutOffsets[F, K, V]] =
    TransactionalKafkaProducer.resource(txnSettings)

  lazy val clientS: Stream[F, TransactionalKafkaProducer.WithoutOffsets[F, K, V]] =
    TransactionalKafkaProducer.stream(txnSettings)
}
