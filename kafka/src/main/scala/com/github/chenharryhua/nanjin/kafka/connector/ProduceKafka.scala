package com.github.chenharryhua.nanjin.kafka.connector

import cats.effect.kernel.{Async, Resource}
import cats.syntax.flatMap.given
import cats.syntax.foldable.given
import cats.{Endo, Foldable}
import com.github.chenharryhua.nanjin.common.{HasProperties, UpdateConfig}
import com.github.chenharryhua.nanjin.kafka.TopicName
import fs2.kafka.*
import fs2.{Chunk, Pipe, Stream}
import org.apache.kafka.clients.producer.RecordMetadata

/*
 * Kafka Sink
 */
final class ProduceKafka[F[_], K, V] private[kafka] (
  topicName: TopicName,
  producerSettings: ProducerSettings[F, K, V])(using F: Async[F])
    extends UpdateConfig[ProducerSettings[F, K, V], ProduceKafka[F, K, V]] with HasProperties
    with ProducerService[F, K, V] {

  /*
   * config
   */
  override lazy val properties: Map[String, String] = producerSettings.properties

  override def updateConfig(f: Endo[ProducerSettings[F, K, V]]): ProduceKafka[F, K, V] =
    new ProduceKafka[F, K, V](topicName, f(producerSettings))

  /*
   * sink
   */
  lazy val clientR: Resource[F, KafkaProducer.Metrics[F, K, V]] =
    KafkaProducer.resource(producerSettings)

  lazy val clientS: Stream[F, KafkaProducer.Metrics[F, K, V]] =
    KafkaProducer.stream(producerSettings)

  override lazy val pairSink: Pipe[F, (K, V), ProducerResult[K, V]] = { (ss: Stream[F, (K, V)]) =>
    clientS.flatMap { producer =>
      ss.chunks.evalMap { ck =>
        producer.produce(ck.map { case (k, v) => ProducerRecord(topicName.value, k, v) })
      }.parEvalMap(Int.MaxValue)(a => a)
    }
  }

  override lazy val sink: Pipe[F, ProducerRecord[K, V], ProducerResult[K, V]] =
    (ss: Stream[F, ProducerRecord[K, V]]) =>
      KafkaProducer.stream[F, K, V](producerSettings).flatMap { producer =>
        ss.chunks.evalMap(producer.produce).parEvalMap(Int.MaxValue)(a => a)
      }

  /*
   * for testing and repl
   */

  override def produce[G[_]: Foldable](kvs: G[(K, V)]): F[ProducerResult[K, V]] = {
    val prs = Chunk.from(kvs.toList).map { case (k, v) => ProducerRecord(topicName.value, k, v) }
    clientR.use(_.produce(prs).flatten)
  }

  override def produceOne(k: K, v: V): F[RecordMetadata] =
    clientR.use(_.produceOne_(topicName.value, k, v).flatten)

  override def produceOne(record: ProducerRecord[K, V]): F[RecordMetadata] =
    clientR.use(_.produceOne_(record).flatten)

  override def transactional(transactionalId: String): KafkaTransactional[F, K, V] =
    new KafkaTransactional[F, K, V](TransactionalProducerSettings(transactionalId, producerSettings))
}
