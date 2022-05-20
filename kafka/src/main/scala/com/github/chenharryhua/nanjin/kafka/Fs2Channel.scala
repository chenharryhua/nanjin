package com.github.chenharryhua.nanjin.kafka

import cats.data.NonEmptyList
import cats.Endo
import cats.effect.kernel.*
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.datetime.NJDateTimeRange
import com.github.chenharryhua.nanjin.messages.kafka.NJConsumerMessage
import com.github.chenharryhua.nanjin.messages.kafka.codec.KafkaGenericDecoder
import fs2.{Pipe, Stream}
import fs2.kafka.*

/** Best Fs2 Kafka Lib [[https://fd4s.github.io/fs2-kafka/]]
  */
final class Fs2Channel[F[_], K, V] private[kafka] (
  val topic: KafkaTopic[F, K, V],
  kps: KafkaProducerSettings,
  kcs: KafkaConsumerSettings,
  csUpdater: fs2Updater.Consumer[F],
  psUpdater: fs2Updater.Producer[F, K, V],
  txnUpdater: fs2Updater.TxnProducer[F, K, V]) {

  val topicName: TopicName = topic.topicName

  // settings

  def updateConsumer(f: Endo[ConsumerSettings[F, Array[Byte], Array[Byte]]]): Fs2Channel[F, K, V] =
    new Fs2Channel[F, K, V](topic, kps, kcs, csUpdater.updateConfig(f), psUpdater, txnUpdater)

  def updateProducer(f: Endo[ProducerSettings[F, K, V]]): Fs2Channel[F, K, V] =
    new Fs2Channel[F, K, V](topic, kps, kcs, csUpdater, psUpdater.updateConfig(f), txnUpdater)

  def updateTxnProducer(f: Endo[TransactionalProducerSettings[F, K, V]]): Fs2Channel[F, K, V] =
    new Fs2Channel[F, K, V](topic, kps, kcs, csUpdater, psUpdater, txnUpdater.updateConfig(f))

  def producerSettings(implicit F: Sync[F]): ProducerSettings[F, K, V] =
    psUpdater.updates(
      ProducerSettings[F, K, V](
        Serializer.delegate(topic.codec.keySerializer),
        Serializer.delegate(topic.codec.valSerializer)).withProperties(kps.config))

  def txnProducerSettings(transactionalId: String)(implicit F: Sync[F]): TransactionalProducerSettings[F, K, V] =
    txnUpdater.updates(TransactionalProducerSettings(transactionalId, producerSettings))

  def consumerSettings(implicit F: Sync[F]): ConsumerSettings[F, Array[Byte], Array[Byte]] =
    csUpdater.updates(
      ConsumerSettings[F, Array[Byte], Array[Byte]](Deserializer[F, Array[Byte]], Deserializer[F, Array[Byte]])
        .withProperties(kcs.config))

  @inline def decoder[G[_, _]: NJConsumerMessage](cr: G[Array[Byte], Array[Byte]]): KafkaGenericDecoder[G, K, V] =
    topic.decoder[G](cr)

  // pipe
  def producerPipe(implicit F: Async[F]): Pipe[F, ProducerRecords[K, V], ProducerResult[K, V]] =
    KafkaProducer.pipe[F, K, V](producerSettings)

  def producer(implicit F: Async[F]): Stream[F, KafkaProducer.Metrics[F, K, V]] =
    KafkaProducer.stream(producerSettings)

  def txnProducer(transactionalId: String)(implicit F: Async[F]): Stream[F, TransactionalKafkaProducer[F, K, V]] =
    TransactionalKafkaProducer.stream(txnProducerSettings(transactionalId))

  def producerResource(implicit F: Async[F]): Resource[F, KafkaProducer.Metrics[F, K, V]] =
    KafkaProducer.resource(producerSettings)

  // sources
  def stream(implicit F: Async[F]): Stream[F, CommittableConsumerRecord[F, Array[Byte], Array[Byte]]] =
    KafkaConsumer
      .stream[F, Array[Byte], Array[Byte]](consumerSettings)
      .evalTap(_.subscribe(NonEmptyList.of(topic.topicName.value)))
      .flatMap(_.stream)

  def assign(tps: KafkaTopicPartition[KafkaOffset])(implicit
    F: Async[F]): Stream[F, CommittableConsumerRecord[F, Array[Byte], Array[Byte]]] =
    if (tps.isEmpty)
      Stream.empty.covaryAll[F, CommittableConsumerRecord[F, Array[Byte], Array[Byte]]]
    else
      KafkaConsumer
        .stream[F, Array[Byte], Array[Byte]](consumerSettings)
        .evalTap { c =>
          c.assign(topic.topicName.value) *> tps.value.toList.traverse { case (tp, offset) =>
            c.seek(tp, offset.offset.value)
          }
        }
        .flatMap(_.stream)
}
