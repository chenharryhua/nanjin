package com.github.chenharryhua.nanjin.kafka.connector

import cats.effect.kernel.*
import cats.implicits.{catsSyntaxFlatten, toFoldableOps}
import cats.{Endo, Foldable}
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.common.{HasProperties, UpdateConfig}
import fs2.kafka.*
import fs2.{Chunk, Pipe, Stream}
import org.apache.kafka.clients.producer.RecordMetadata
final class ProduceKafka[F[_], K, V] private[kafka] (
  topicName: TopicName,
  producerSettings: ProducerSettings[F, K, V],
  isCompatible: F[Boolean])
    extends UpdateConfig[ProducerSettings[F, K, V], ProduceKafka[F, K, V]] with HasProperties {

  /*
   * config
   */
  override def properties: Map[String, String] = producerSettings.properties

  override def updateConfig(f: Endo[ProducerSettings[F, K, V]]): ProduceKafka[F, K, V] =
    new ProduceKafka[F, K, V](topicName, f(producerSettings), isCompatible)

  private def kafkaProducer(implicit F: Async[F]): Resource[F, KafkaProducer[F, K, V]] =
    Resource.eval(isCompatible).flatMap {
      case false =>
        Resource.raiseError[F, KafkaProducer.PartitionsFor[F, K, V], Throwable](
          new Exception("incompatible schema"))
      case true => KafkaProducer.resource(producerSettings)
    }

  /*
   * sink
   */

  def sink(implicit F: Async[F]): Pipe[F, Chunk[(K, V)], ProducerResult[K, V]] = {
    (ss: Stream[F, Chunk[(K, V)]]) =>
      Stream.resource(kafkaProducer).flatMap { producer =>
        ss.evalMap { ck =>
          producer.produce(ck.map { case (k, v) => ProducerRecord(topicName.name.value, k, v) })
        }.parEvalMap(Int.MaxValue)(identity)
      }
  }

  /*
   * for testing and repl
   */

  def produce[G[_]: Foldable](kvs: G[(K, V)])(implicit F: Async[F]): F[ProducerResult[K, V]] = {
    val prs = Chunk.from(kvs.toList).map { case (k, v) => ProducerRecord(topicName.name.value, k, v) }
    kafkaProducer.use(_.produce(prs).flatten)
  }

  def produceOne(k: K, v: V)(implicit F: Async[F]): F[RecordMetadata] =
    kafkaProducer.use(_.produceOne_(topicName.name.value, k, v).flatten)
}
