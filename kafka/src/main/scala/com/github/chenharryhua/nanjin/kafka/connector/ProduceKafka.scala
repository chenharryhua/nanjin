package com.github.chenharryhua.nanjin.kafka.connector

import cats.effect.kernel.*
import cats.implicits.{catsSyntaxFlatten, toFoldableOps, toFunctorOps}
import cats.{Endo, Foldable}
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.common.{HasProperties, UpdateConfig}
import fs2.kafka.*
import fs2.{Chunk, Pipe, Stream}
import org.apache.kafka.clients.producer.RecordMetadata

/*
 * Kafka Sink
 */
final class ProduceKafka[F[_], K, V] private[kafka] (
  topicName: TopicName,
  producerSettings: ProducerSettings[F, K, V],
  isCompatible: F[Boolean])(implicit F: Async[F])
    extends UpdateConfig[ProducerSettings[F, K, V], ProduceKafka[F, K, V]] with HasProperties {

  /*
   * config
   */
  override def properties: Map[String, String] = producerSettings.properties

  override def updateConfig(f: Endo[ProducerSettings[F, K, V]]): ProduceKafka[F, K, V] =
    new ProduceKafka[F, K, V](topicName, f(producerSettings), isCompatible)

  private lazy val kafkaProducer: Resource[F, KafkaProducer[F, K, V]] =
    Resource.eval(isCompatible).flatMap {
      case false =>
        Resource.raiseError[F, KafkaProducer.PartitionsFor[F, K, V], Throwable](
          new Exception("incompatible schema"))
      case true => KafkaProducer.resource(producerSettings)
    }

  /*
   * sink
   */

  lazy val sink: Pipe[F, (K, V), Chunk[RecordMetadata]] = { (ss: Stream[F, (K, V)]) =>
    Stream.resource(kafkaProducer).flatMap { producer =>
      ss.chunks.evalMap { ck =>
        producer.produce(ck.map { case (k, v) => ProducerRecord(topicName.name.value, k, v) })
      }.parEvalMap(Int.MaxValue)(_.map(_.map(_._2)))
    }
  }

  /*
   * for testing and repl
   */

  def produce[G[_]: Foldable](kvs: G[(K, V)]): F[Chunk[RecordMetadata]] = {
    val prs = Chunk.from(kvs.toList).map { case (k, v) => ProducerRecord(topicName.name.value, k, v) }
    kafkaProducer.use(_.produce(prs).flatten).map(_.map(_._2))
  }

  def produceOne(k: K, v: V): F[RecordMetadata] =
    kafkaProducer.use(_.produceOne_(topicName.name.value, k, v).flatten)
}
