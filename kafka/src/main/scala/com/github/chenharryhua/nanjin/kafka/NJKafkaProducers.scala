package com.github.chenharryhua.nanjin.kafka

import cats.Endo
import cats.effect.kernel.*
import com.github.chenharryhua.nanjin.common.UpdateConfig
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import fs2.kafka.*
import fs2.{Pipe, Stream}
import org.apache.avro.generic.GenericRecord

/** Best Fs2 Kafka Lib [[https://fd4s.github.io/fs2-kafka/]]
  *
  * [[https://redpanda.com/guides/kafka-performance/kafka-performance-tuning]]
  */

final class NJKafkaProduce[F[_], K, V] private[kafka] (producerSettings: ProducerSettings[F, K, V])
    extends UpdateConfig[ProducerSettings[F, K, V], NJKafkaProduce[F, K, V]] {
  def transactional(transactionalId: String): NJKafkaTransactional[F, K, V] =
    new NJKafkaTransactional[F, K, V](TransactionalProducerSettings(transactionalId, producerSettings))

  def clientR(implicit F: Async[F]): Resource[F, KafkaProducer.Metrics[F, K, V]] =
    KafkaProducer.resource(producerSettings)

  def pipe(implicit F: Async[F]): Pipe[F, ProducerRecords[K, V], ProducerResult[K, V]] =
    KafkaProducer.pipe[F, K, V](producerSettings)

  def client(implicit F: Async[F]): Stream[F, KafkaProducer.Metrics[F, K, V]] =
    KafkaProducer.stream(producerSettings)

  override def updateConfig(f: Endo[ProducerSettings[F, K, V]]): NJKafkaProduce[F, K, V] =
    new NJKafkaProduce[F, K, V](f(producerSettings))
}

final class NJKafkaTransactional[F[_], K, V] private[kafka] (
  txnSettings: TransactionalProducerSettings[F, K, V])
    extends UpdateConfig[TransactionalProducerSettings[F, K, V], NJKafkaTransactional[F, K, V]] {
  def client(implicit F: Async[F]): Stream[F, TransactionalKafkaProducer[F, K, V]] =
    TransactionalKafkaProducer.stream(txnSettings)

  override def updateConfig(f: Endo[TransactionalProducerSettings[F, K, V]]): NJKafkaTransactional[F, K, V] =
    new NJKafkaTransactional[F, K, V](f(txnSettings))
}

final class NJGenericRecordSink[F[_]] private[kafka] (
  topicName: TopicName,
  producerSettings: ProducerSettings[F, Array[Byte], Array[Byte]],
  getSchema: F[AvroSchemaPair],
  srs: SchemaRegistrySettings)
    extends UpdateConfig[ProducerSettings[F, Array[Byte], Array[Byte]], NJGenericRecordSink[F]] {

  override def updateConfig(f: Endo[ProducerSettings[F, Array[Byte], Array[Byte]]]): NJGenericRecordSink[F] =
    new NJGenericRecordSink[F](topicName, f(producerSettings), getSchema, srs)

  def build(implicit F: Async[F]): Pipe[F, GenericRecord, ProducerResult[Array[Byte], Array[Byte]]] = {
    (ss: Stream[F, GenericRecord]) =>
      Stream.eval(getSchema).flatMap { skm =>
        val builder = new PushGenericRecord(srs, topicName, skm)
        val prStream: Stream[F, ProducerRecords[Array[Byte], Array[Byte]]] =
          ss.map(builder.fromGenericRecord).chunks
        KafkaProducer.pipe(producerSettings).apply(prStream)
      }
  }
}
