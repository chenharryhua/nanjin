package com.github.chenharryhua.nanjin.kafka

import akka.actor.ActorSystem
import cats.effect.{ConcurrentEffect, ContextShift, Resource, Timer}
import cats.implicits._
import cats.{Show, Traverse}
import com.github.chenharryhua.nanjin.kafka.api._
import com.github.chenharryhua.nanjin.kafka.codec.{KafkaGenericDecoder, NJConsumerMessage}
import fs2.kafka.{KafkaProducer, ProducerResult}
import org.apache.kafka.streams.processor.{RecordContext, TopicNameExtractor}

final class KafkaTopic[F[_], K, V] private[kafka] (val description: KafkaTopicDescription[K, V])(
  implicit
  val concurrentEffect: ConcurrentEffect[F],
  val timer: Timer[F],
  val contextShift: ContextShift[F]
) extends TopicNameExtractor[K, V] {
  import description.topicDef.{showKey, showValue}

  val topicName: String = description.topicDef.topicName

  val consumerGroupId: Option[KafkaConsumerGroupId] = description.consumerGroupId

  override def extract(key: K, value: V, rc: RecordContext): String = topicName

  def decoder[G[_, _]: NJConsumerMessage](
    cr: G[Array[Byte], Array[Byte]]): KafkaGenericDecoder[G, K, V] =
    description.decoder(cr)

  //channels
  def fs2Channel: KafkaChannels.Fs2Channel[F, K, V] =
    new KafkaChannels.Fs2Channel[F, K, V](
      description.topicDef.topicName,
      description.fs2ProducerSettings,
      description.fs2ConsumerSettings)

  def akkaResource(akkaSystem: ActorSystem): Resource[F, KafkaChannels.AkkaChannel[F, K, V]] =
    Resource.make(
      ConcurrentEffect[F].delay(
        new KafkaChannels.AkkaChannel[F, K, V](
          description.topicDef.topicName,
          description.akkaProducerSettings(akkaSystem),
          description.akkaConsumerSettings(akkaSystem),
          description.akkaCommitterSettings(akkaSystem)
        )))(_ => ConcurrentEffect[F].unit)

  def kafkaStream: KafkaChannels.StreamingChannel[K, V] =
    new KafkaChannels.StreamingChannel[K, V](
      description.topicDef.topicName,
      description.codec.keySerde,
      description.codec.valueSerde)

  private val pr: Resource[F, KafkaProducer[F, K, V]] =
    fs2.kafka.producerResource[F].using(description.fs2ProducerSettings)

  def send(k: K, v: V): F[ProducerResult[K, V, Unit]] =
    pr.use(_.produce(description.fs2ProducerRecords(k, v))).flatten

  def send[G[+_]: Traverse](list: G[(K, V)]): F[ProducerResult[K, V, Unit]] =
    pr.use(_.produce(description.fs2ProducerRecords(list))).flatten

  // APIs
  val schemaRegistry: KafkaSchemaRegistryApi[F] = api.KafkaSchemaRegistryApi[F](this.description)
  val admin: KafkaTopicAdminApi[F]              = api.KafkaTopicAdminApi[F, K, V](this.description)

  val consumerResource: Resource[F, KafkaConsumerApi[F]] = api.KafkaConsumerApi(this.description)

  val monitor: KafkaMonitoringApi[F, K, V] =
    api.KafkaMonitoringApi[F, K, V](this)
}

object KafkaTopic {
  implicit def showKafkaTopic[F[_], K, V]: Show[KafkaTopic[F, K, V]] = _.topicName
}
