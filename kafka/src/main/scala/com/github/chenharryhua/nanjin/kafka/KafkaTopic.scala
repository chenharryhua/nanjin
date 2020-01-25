package com.github.chenharryhua.nanjin.kafka

import akka.actor.ActorSystem
import cats.Traverse
import cats.effect.{ConcurrentEffect, ContextShift, Resource, Timer}
import cats.implicits._
import com.github.chenharryhua.nanjin.kafka.codec.{KafkaGenericDecoder, NJConsumerMessage}
import com.github.chenharryhua.nanjin.kafka.data.{KafkaConsumerGroupId, TopicName}
import fs2.kafka.{KafkaProducer, ProducerRecord, ProducerRecords, ProducerResult}
import org.apache.kafka.streams.processor.{RecordContext, TopicNameExtractor}

final class KafkaTopic[F[_], K, V] private[kafka] (val description: KafkaTopicDescription[K, V])(
  implicit
  val concurrentEffect: ConcurrentEffect[F],
  val timer: Timer[F],
  val contextShift: ContextShift[F]
) extends TopicNameExtractor[K, V] {
  import description.topicDef.{showKey, showValue}

  val topicName: TopicName = description.topicDef.topicName

  val consumerGroupId: Option[KafkaConsumerGroupId] = description.consumerGroupId

  override def extract(key: K, value: V, rc: RecordContext): String = topicName.value

  def decoder[G[_, _]: NJConsumerMessage](
    cr: G[Array[Byte], Array[Byte]]): KafkaGenericDecoder[G, K, V] =
    description.decoder(cr)

  //channels
  def fs2Channel: KafkaChannels.Fs2Channel[F, K, V] =
    new KafkaChannels.Fs2Channel[F, K, V](
      description.topicDef.topicName,
      description.fs2ProducerSettings,
      description.fs2ConsumerSettings)

  def akkaChannel(akkaSystem: ActorSystem): KafkaChannels.AkkaChannel[F, K, V] =
    new KafkaChannels.AkkaChannel[F, K, V](
      description.topicDef.topicName,
      description.akkaProducerSettings(akkaSystem),
      description.akkaConsumerSettings(akkaSystem),
      description.akkaCommitterSettings(akkaSystem))

  def kafkaStream: KafkaChannels.StreamingChannel[K, V] =
    new KafkaChannels.StreamingChannel[K, V](
      description.topicDef.topicName,
      description.codec.keySerde,
      description.codec.valueSerde)

  private val fs2ProducerResource: Resource[F, KafkaProducer[F, K, V]] =
    fs2.kafka.producerResource[F].using(description.fs2ProducerSettings)

  def fs2PR(k: K, v: V): ProducerRecord[K, V] =
    description.fs2PR(k, v)

  def send(k: K, v: V): F[ProducerResult[K, V, Unit]] =
    fs2ProducerResource.use(_.produce(description.fs2ProducerRecords(k, v))).flatten

  def send[G[+_]: Traverse](list: G[(K, V)]): F[ProducerResult[K, V, Unit]] =
    fs2ProducerResource.use(_.produce(description.fs2ProducerRecords(list))).flatten

  def send(pr: ProducerRecord[K, V]): F[ProducerResult[K, V, Unit]] =
    fs2ProducerResource.use(_.produce(ProducerRecords.one(pr))).flatten

  // APIs
  val schemaRegistry: KafkaSchemaRegistryApi[F]          = KafkaSchemaRegistryApi[F](this.description)
  val admin: KafkaTopicAdminApi[F]                       = KafkaTopicAdminApi[F, K, V](this.description)
  val consumerResource: Resource[F, KafkaConsumerApi[F]] = KafkaConsumerApi(this.description)
  val monitor: KafkaMonitoringApi[F, K, V]               = KafkaMonitoringApi[F, K, V](this)

  override def toString: String = description.toString
}
