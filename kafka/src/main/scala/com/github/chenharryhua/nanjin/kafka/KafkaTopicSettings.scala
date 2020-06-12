package com.github.chenharryhua.nanjin.kafka

import akka.actor.ActorSystem
import akka.kafka.{
  CommitterSettings => AkkaCommitterSettings,
  ConsumerSettings => AkkaConsumerSettings,
  ProducerSettings => AkkaProducerSettings
}
import cats.effect.{ConcurrentEffect, ContextShift, Sync, Timer}
import cats.implicits._
import fs2.kafka.{
  ConsumerSettings => Fs2ConsumerSettings,
  Deserializer => Fs2Deserializer,
  ProducerSettings => Fs2ProducerSettings,
  Serializer => Fs2Serializer
}
import org.apache.kafka.common.serialization.ByteArrayDeserializer

private[kafka] trait KafkaTopicSettings[F[_], K, V] { topic: KafkaTopic[F, K, V] =>

  def fs2ProducerSettings(implicit ev: Sync[F]): Fs2ProducerSettings[F, K, V] =
    Fs2ProducerSettings[F, K, V](
      Fs2Serializer.delegate(codec.keySerializer),
      Fs2Serializer.delegate(codec.valSerializer))
      .withProperties(topic.settings.producerSettings.config)

  def fs2ConsumerSettings(implicit ev: Sync[F]): Fs2ConsumerSettings[F, Array[Byte], Array[Byte]] =
    Fs2ConsumerSettings[F, Array[Byte], Array[Byte]](
      Fs2Deserializer[F, Array[Byte]],
      Fs2Deserializer[F, Array[Byte]]).withProperties(topic.settings.consumerSettings.config)

  def akkaProducerSettings(implicit akkaSystem: ActorSystem): AkkaProducerSettings[K, V] =
    AkkaProducerSettings[K, V](akkaSystem, codec.keySerializer, codec.valSerializer)
      .withProperties(topic.settings.producerSettings.config)

  def akkaConsumerSettings(implicit
    akkaSystem: ActorSystem): AkkaConsumerSettings[Array[Byte], Array[Byte]] = {
    val byteArrayDeserializer = new ByteArrayDeserializer
    AkkaConsumerSettings[Array[Byte], Array[Byte]](
      akkaSystem,
      byteArrayDeserializer,
      byteArrayDeserializer).withProperties(topic.settings.consumerSettings.config)
  }

  def akkaCommitterSettings(implicit akkaSystem: ActorSystem): AkkaCommitterSettings =
    AkkaCommitterSettings(akkaSystem)

  def fs2Channel(implicit
    concurrentEffect: ConcurrentEffect[F],
    timer: Timer[F],
    contextShift: ContextShift[F]): KafkaChannels.Fs2Channel[F, K, V] =
    new KafkaChannels.Fs2Channel[F, K, V](
      topic.topicDef.topicName,
      fs2ProducerSettings,
      fs2ConsumerSettings)

  def akkaChannel(implicit
    akkaSystem: ActorSystem,
    concurrentEffect: ConcurrentEffect[F],
    contextShift: ContextShift[F]): KafkaChannels.AkkaChannel[F, K, V] =
    new KafkaChannels.AkkaChannel[F, K, V](
      topicName,
      akkaProducerSettings(akkaSystem),
      akkaConsumerSettings(akkaSystem),
      akkaCommitterSettings(akkaSystem))

  def kafkaStream: KafkaChannels.StreamingChannel[K, V] =
    new KafkaChannels.StreamingChannel[K, V](
      topic.topicDef.topicName,
      codec.keySerde,
      codec.valSerde)

  // schema registry operations
  def schemaRegister(implicit F: Sync[F]): F[(Option[Int], Option[Int])] =
    new SchemaRegistryApi[F](settings.schemaRegistrySettings)
      .register(topicName, topicDef.keySchemaFor.schema, topicDef.valSchemaFor.schema)

  def schemaDelete(implicit F: Sync[F]): F[(List[Integer], List[Integer])] =
    new SchemaRegistryApi[F](settings.schemaRegistrySettings).delete(topicName)

  def schemaCompatibility(implicit F: Sync[F]): F[CompatibilityTestReport] =
    new SchemaRegistryApi[F](settings.schemaRegistrySettings)
      .testCompatibility(topicName, topicDef.keySchemaFor.schema, topicDef.valSchemaFor.schema)

}
