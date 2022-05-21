package com.github.chenharryhua.nanjin.kafka

import akka.{Done, NotUsed}
import akka.kafka.*
import akka.kafka.ConsumerMessage.{Committable, CommittableMessage, TransactionalMessage}
import akka.kafka.ProducerMessage.{Envelope, Results}
import akka.kafka.scaladsl.{Consumer, Producer, Transactional}
import akka.stream.scaladsl.{Flow, Sink, Source}
import cats.Endo
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.common.UpdateConfig
import fs2.kafka.KafkaByteConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord

import scala.concurrent.Future

/** [[https://doc.akka.io/docs/alpakka-kafka/current/home.html]]
  */

final class AkkaConsume private[kafka] (
  topicName: TopicName,
  consumerSettings: ConsumerSettings[Array[Byte], Array[Byte]])
    extends UpdateConfig[ConsumerSettings[Array[Byte], Array[Byte]], AkkaConsume] {

  def assign(tps: KafkaTopicPartition[KafkaOffset]): Source[KafkaByteConsumerRecord, Consumer.Control] =
    if (tps.isEmpty)
      Source.empty.mapMaterializedValue(_ => Consumer.NoopControl)
    else
      Consumer.plainSource(
        consumerSettings,
        Subscriptions.assignmentWithOffset(tps.value.view.mapValues(_.offset.value).toMap))

  val source: Source[CommittableMessage[Array[Byte], Array[Byte]], Consumer.Control] =
    Consumer.committableSource(consumerSettings, Subscriptions.topics(topicName.value))

  val transactional: Source[TransactionalMessage[Array[Byte], Array[Byte]], Consumer.Control] =
    Transactional.source(consumerSettings, Subscriptions.topics(topicName.value))

  override def updateConfig(f: Endo[ConsumerSettings[Array[Byte], Array[Byte]]]): AkkaConsume =
    new AkkaConsume(topicName, f(consumerSettings))
}

final class AkkaProduce[K, V] private[kafka] (producerSettings: ProducerSettings[K, V])
    extends UpdateConfig[ProducerSettings[K, V], AkkaProduce[K, V]] {

  def flexiFlow[P]: Flow[Envelope[K, V, P], Results[K, V, P], NotUsed] =
    Producer.flexiFlow[K, V, P](producerSettings)

  def plainSink: Sink[ProducerRecord[K, V], Future[Done]] = Producer.plainSink(producerSettings)

  def committableSink(committerSettings: CommitterSettings): Sink[Envelope[K, V, Committable], Future[Done]] =
    Producer.committableSink(producerSettings, committerSettings)

  override def updateConfig(f: Endo[ProducerSettings[K, V]]): AkkaProduce[K, V] =
    new AkkaProduce[K, V](f(producerSettings))
}
