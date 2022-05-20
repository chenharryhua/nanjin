package com.github.chenharryhua.nanjin.kafka

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.kafka.*
import akka.kafka.ConsumerMessage.{Committable, CommittableMessage, TransactionalMessage}
import akka.kafka.ProducerMessage.{Envelope, Results}
import akka.kafka.scaladsl.{Committer, Consumer, Producer, Transactional}
import akka.stream.scaladsl.{Flow, Sink, Source}
import cats.Endo
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.messages.kafka.NJConsumerMessage
import com.github.chenharryhua.nanjin.messages.kafka.codec.KafkaGenericDecoder
import fs2.kafka.KafkaByteConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArrayDeserializer

import scala.concurrent.Future

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

  val topicName: TopicName = topic.topicName
  // settings
  def updateConsumer(f: Endo[ConsumerSettings[Array[Byte], Array[Byte]]]): AkkaChannel[F, K, V] =
    new AkkaChannel[F, K, V](topic, akkaSystem, kps, kcs, csUpdater.updateConfig(f), psUpdater, ctUpdater)

  def updateProducer(f: Endo[ProducerSettings[K, V]]): AkkaChannel[F, K, V] =
    new AkkaChannel[F, K, V](topic, akkaSystem, kps, kcs, csUpdater, psUpdater.updateConfig(f), ctUpdater)

  def updateCommitter(f: Endo[CommitterSettings]): AkkaChannel[F, K, V] =
    new AkkaChannel[F, K, V](topic, akkaSystem, kps, kcs, csUpdater, psUpdater, ctUpdater.updateConfig(f))

  def producerSettings: ProducerSettings[K, V] =
    psUpdater.updates(
      ProducerSettings[K, V](akkaSystem, topic.codec.keySerializer, topic.codec.valSerializer)
        .withProperties(kps.config))

  def consumerSettings: ConsumerSettings[Array[Byte], Array[Byte]] =
    csUpdater.updates(
      ConsumerSettings[Array[Byte], Array[Byte]](akkaSystem, new ByteArrayDeserializer, new ByteArrayDeserializer)
        .withProperties(kcs.config))

  def committerSettings: CommitterSettings =
    ctUpdater.updates(CommitterSettings(akkaSystem))

  @inline def decoder[G[_, _]: NJConsumerMessage](cr: G[Array[Byte], Array[Byte]]): KafkaGenericDecoder[G, K, V] =
    topic.decoder[G](cr)

  // flow
  def flexiFlow[P]: Flow[Envelope[K, V, P], Results[K, V, P], NotUsed] =
    Producer.flexiFlow[K, V, P](producerSettings)

  // sinks
  def committableSink: Sink[Envelope[K, V, Committable], Future[Done]] =
    Producer.committableSink(producerSettings, committerSettings)
  def plainSink: Sink[ProducerRecord[K, V], Future[Done]] = Producer.plainSink(producerSettings)
  def commitSink: Sink[Committable, Future[Done]]         = Committer.sink(committerSettings)

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

  val transactionalSource: Source[TransactionalMessage[K, V], Consumer.Control] =
    Transactional.source(consumerSettings, Subscriptions.topics(topicName.value)).map(decoder(_).decode)
}
