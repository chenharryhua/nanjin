package com.github.chenharryhua.nanjin.kafka

import akka.actor.ActorSystem
import akka.{Done, NotUsed}
import cats.data.NonEmptyList
import cats.effect.kernel.*
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.datetime.NJDateTimeRange
import com.github.chenharryhua.nanjin.messages.kafka.NJConsumerMessage
import com.github.chenharryhua.nanjin.messages.kafka.codec.KafkaGenericDecoder
import fs2.{Pipe, Stream}
import fs2.kafka.KafkaByteConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArrayDeserializer

import scala.concurrent.Future

object KafkaChannels {

  /** Best Fs2 Kafka Lib [[https://fd4s.github.io/fs2-kafka/]]
    */
  final class Fs2Channel[F[_], K, V] private[kafka] (
    val topic: KafkaTopic[F, K, V],
    kps: KafkaProducerSettings,
    kcs: KafkaConsumerSettings,
    csUpdater: fs2Updater.Consumer[F],
    psUpdater: fs2Updater.Producer[F, K, V],
    txnUpdater: fs2Updater.TxnProducer[F, K, V]) {
    import fs2.kafka.{
      CommittableConsumerRecord,
      ConsumerSettings,
      Deserializer,
      KafkaConsumer,
      KafkaProducer,
      ProducerRecords,
      ProducerResult,
      ProducerSettings,
      Serializer,
      TransactionalKafkaProducer,
      TransactionalProducerSettings
    }

    val topicName: TopicName = topic.topicName

    // settings

    def updateConsumer(
      f: ConsumerSettings[F, Array[Byte], Array[Byte]] => ConsumerSettings[F, Array[Byte], Array[Byte]])
      : Fs2Channel[F, K, V] =
      new Fs2Channel[F, K, V](topic, kps, kcs, csUpdater.updateConfig(f), psUpdater, txnUpdater)

    def updateProducer(f: ProducerSettings[F, K, V] => ProducerSettings[F, K, V]): Fs2Channel[F, K, V] =
      new Fs2Channel[F, K, V](topic, kps, kcs, csUpdater, psUpdater.updateConfig(f), txnUpdater)

    def updateTxnProducer(
      f: TransactionalProducerSettings[F, K, V] => TransactionalProducerSettings[F, K, V]): Fs2Channel[F, K, V] =
      new Fs2Channel[F, K, V](topic, kps, kcs, csUpdater, psUpdater, txnUpdater.updateConfig(f))

    def producerSettings(implicit F: Sync[F]): ProducerSettings[F, K, V] =
      psUpdater.updates.run(
        ProducerSettings[F, K, V](
          Serializer.delegate(topic.codec.keySerializer),
          Serializer.delegate(topic.codec.valSerializer)).withProperties(kps.config))

    def txnProducerSettings(transactionalId: String)(implicit F: Sync[F]): TransactionalProducerSettings[F, K, V] =
      txnUpdater.updates.run(TransactionalProducerSettings(transactionalId, producerSettings))

    def consumerSettings(implicit F: Sync[F]): ConsumerSettings[F, Array[Byte], Array[Byte]] =
      csUpdater.updates.run(
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

  /** [[https://doc.akka.io/docs/alpakka-kafka/current/home.html]]
    */
  final class AkkaChannel[F[_], K, V] private[kafka] (
    val topic: KafkaTopic[F, K, V],
    akkaSystem: ActorSystem,
    kps: KafkaProducerSettings,
    kcs: KafkaConsumerSettings,
    csUpdater: akkaUpdater.Consumer,
    psUpdater: akkaUpdater.Producer[K, V],
    ctUpdater: akkaUpdater.Committer) {
    import akka.kafka.*
    import akka.kafka.ConsumerMessage.CommittableMessage
    import akka.kafka.ProducerMessage.Envelope
    import akka.kafka.scaladsl.{Committer, Consumer, Producer, Transactional}
    import akka.stream.scaladsl.{Flow, Sink, Source}

    val topicName: TopicName = topic.topicName
    // settings
    def updateConsumer(f: ConsumerSettings[Array[Byte], Array[Byte]] => ConsumerSettings[Array[Byte], Array[Byte]])
      : AkkaChannel[F, K, V] =
      new AkkaChannel[F, K, V](topic, akkaSystem, kps, kcs, csUpdater.updateConfig(f), psUpdater, ctUpdater)

    def updateProducer(f: ProducerSettings[K, V] => ProducerSettings[K, V]): AkkaChannel[F, K, V] =
      new AkkaChannel[F, K, V](topic, akkaSystem, kps, kcs, csUpdater, psUpdater.updateConfig(f), ctUpdater)

    def updateCommitter(f: CommitterSettings => CommitterSettings): AkkaChannel[F, K, V] =
      new AkkaChannel[F, K, V](topic, akkaSystem, kps, kcs, csUpdater, psUpdater, ctUpdater.updateConfig(f))

    def producerSettings: ProducerSettings[K, V] =
      psUpdater.updates.run(
        ProducerSettings[K, V](akkaSystem, topic.codec.keySerializer, topic.codec.valSerializer)
          .withProperties(kps.config))

    def consumerSettings: ConsumerSettings[Array[Byte], Array[Byte]] =
      csUpdater.updates.run(
        ConsumerSettings[Array[Byte], Array[Byte]](akkaSystem, new ByteArrayDeserializer, new ByteArrayDeserializer)
          .withProperties(kcs.config))

    def committerSettings: CommitterSettings =
      ctUpdater.updates.run(CommitterSettings(akkaSystem))

    @inline def decoder[G[_, _]: NJConsumerMessage](cr: G[Array[Byte], Array[Byte]]): KafkaGenericDecoder[G, K, V] =
      topic.decoder[G](cr)

    // flow
    def flexiFlow[P]: Flow[Envelope[K, V, P], ProducerMessage.Results[K, V, P], NotUsed] =
      Producer.flexiFlow[K, V, P](producerSettings)

    // sinks
    def committableSink: Sink[Envelope[K, V, ConsumerMessage.Committable], Future[Done]] =
      Producer.committableSink(producerSettings, committerSettings)
    def plainSink: Sink[ProducerRecord[K, V], Future[Done]]         = Producer.plainSink(producerSettings)
    def commitSink: Sink[ConsumerMessage.Committable, Future[Done]] = Committer.sink(committerSettings)

    // sources
    def assign(tps: KafkaTopicPartition[KafkaOffset]): Source[KafkaByteConsumerRecord, Consumer.Control] =
      if (tps.isEmpty)
        Source.empty.mapMaterializedValue(_ => Consumer.NoopControl)
      else
        Consumer.plainSource(
          consumerSettings,
          Subscriptions.assignmentWithOffset(tps.value.view.mapValues(_.offset.value).toMap))

    val source: Source[CommittableMessage[Array[Byte], Array[Byte]], Consumer.Control] =
      Consumer.committableSource(consumerSettings, Subscriptions.topics(topicName.value))

    val transactionalSource: Source[ConsumerMessage.TransactionalMessage[K, V], Consumer.Control] =
      Transactional.source(consumerSettings, Subscriptions.topics(topicName.value)).map(decoder(_).decode)
  }
}
