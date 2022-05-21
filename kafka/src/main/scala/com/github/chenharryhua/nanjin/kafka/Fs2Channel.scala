package com.github.chenharryhua.nanjin.kafka

import cats.data.NonEmptyList
import cats.Endo
import cats.effect.kernel.*
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.common.UpdateConfig
import com.github.chenharryhua.nanjin.datetime.NJDateTimeRange
import fs2.{Pipe, Stream}
import fs2.kafka.*

/** Best Fs2 Kafka Lib [[https://fd4s.github.io/fs2-kafka/]]
  */

final class Fs2Consume[F[_]] private[kafka] (
  topicName: TopicName,
  consumerSettings: ConsumerSettings[F, Array[Byte], Array[Byte]])
    extends UpdateConfig[ConsumerSettings[F, Array[Byte], Array[Byte]], Fs2Consume[F]] {

  def stream(implicit F: Async[F]): Stream[F, CommittableConsumerRecord[F, Array[Byte], Array[Byte]]] =
    KafkaConsumer
      .stream[F, Array[Byte], Array[Byte]](consumerSettings)
      .evalTap(_.subscribe(NonEmptyList.of(topicName.value)))
      .flatMap(_.stream)

  def assign(tps: KafkaTopicPartition[KafkaOffset])(implicit
    F: Async[F]): Stream[F, CommittableConsumerRecord[F, Array[Byte], Array[Byte]]] =
    if (tps.isEmpty)
      Stream.empty.covaryAll[F, CommittableConsumerRecord[F, Array[Byte], Array[Byte]]]
    else
      KafkaConsumer
        .stream[F, Array[Byte], Array[Byte]](consumerSettings)
        .evalTap { c =>
          c.assign(topicName.value) *> tps.value.toList.traverse { case (tp, offset) =>
            c.seek(tp, offset.offset.value)
          }
        }
        .flatMap(_.stream)

  override def updateConfig(f: Endo[ConsumerSettings[F, Array[Byte], Array[Byte]]]): Fs2Consume[F] =
    new Fs2Consume[F](topicName, f(consumerSettings))
}

final class Fs2Produce[F[_], K, V] private[kafka] (producerSettings: ProducerSettings[F, K, V])
    extends UpdateConfig[ProducerSettings[F, K, V], Fs2Produce[F, K, V]] {
  def transactional(transactionalId: String): Fs2Transactional[F, K, V] =
    new Fs2Transactional[F, K, V](TransactionalProducerSettings(transactionalId, producerSettings))

  def producerResource(implicit F: Async[F]): Resource[F, KafkaProducer.Metrics[F, K, V]] =
    KafkaProducer.resource(producerSettings)

  def producerPipe(implicit F: Async[F]): Pipe[F, ProducerRecords[K, V], ProducerResult[K, V]] =
    KafkaProducer.pipe[F, K, V](producerSettings)

  def producer(implicit F: Async[F]): Stream[F, KafkaProducer.Metrics[F, K, V]] =
    KafkaProducer.stream(producerSettings)

  override def updateConfig(f: Endo[ProducerSettings[F, K, V]]): Fs2Produce[F, K, V] =
    new Fs2Produce[F, K, V](f(producerSettings))
}

final class Fs2Transactional[F[_], K, V] private[kafka] (txnSettings: TransactionalProducerSettings[F, K, V])
    extends UpdateConfig[TransactionalProducerSettings[F, K, V], Fs2Transactional[F, K, V]] {
  def txnProducer(implicit F: Async[F]): Stream[F, TransactionalKafkaProducer[F, K, V]] =
    TransactionalKafkaProducer.stream(txnSettings)

  override def updateConfig(f: Endo[TransactionalProducerSettings[F, K, V]]): Fs2Transactional[F, K, V] =
    new Fs2Transactional[F, K, V](f(txnSettings))
}
