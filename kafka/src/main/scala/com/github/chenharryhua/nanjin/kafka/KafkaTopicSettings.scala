package com.github.chenharryhua.nanjin.kafka

import akka.actor.ActorSystem
import akka.kafka.{
  CommitterSettings => AkkaCommitterSettings,
  ConsumerSettings => AkkaConsumerSettings,
  ProducerSettings => AkkaProducerSettings
}
import cats.effect.{ConcurrentEffect, ContextShift, Sync, Timer}
import fs2.kafka.{
  ConsumerSettings => Fs2ConsumerSettings,
  Deserializer => Fs2Deserializer,
  ProducerSettings => Fs2ProducerSettings,
  Serializer => Fs2Serializer
}
import org.apache.kafka.common.serialization.ByteArrayDeserializer

private[kafka] trait KafkaTopicSettings[F[_], K, V] { topic: KafkaTopic[F, K, V] =>

  private def fs2ProducerSettings(implicit ev: Sync[F]): Fs2ProducerSettings[F, K, V] =
    Fs2ProducerSettings[F, K, V](
      Fs2Serializer.delegate(codec.keySerializer),
      Fs2Serializer.delegate(codec.valSerializer)).withProperties(topic.context.settings.producerSettings.config)

  private def fs2ConsumerSettings(implicit ev: Sync[F]): Fs2ConsumerSettings[F, Array[Byte], Array[Byte]] =
    Fs2ConsumerSettings[F, Array[Byte], Array[Byte]](Fs2Deserializer[F, Array[Byte]], Fs2Deserializer[F, Array[Byte]])
      .withProperties(topic.context.settings.consumerSettings.config)

  def fs2Channel(implicit F: Sync[F]): KafkaChannels.Fs2Channel[F, K, V] =
    new KafkaChannels.Fs2Channel[F, K, V](topic.topicDef.topicName, fs2ProducerSettings, fs2ConsumerSettings)

  private def akkaProducerSettings(akkaSystem: ActorSystem): AkkaProducerSettings[K, V] =
    AkkaProducerSettings[K, V](akkaSystem, codec.keySerializer, codec.valSerializer)
      .withProperties(topic.context.settings.producerSettings.config)

  private def akkaConsumerSettings(akkaSystem: ActorSystem): AkkaConsumerSettings[Array[Byte], Array[Byte]] = {
    val byteArrayDeserializer = new ByteArrayDeserializer
    AkkaConsumerSettings[Array[Byte], Array[Byte]](akkaSystem, byteArrayDeserializer, byteArrayDeserializer)
      .withProperties(topic.context.settings.consumerSettings.config)
  }

  private def akkaCommitterSettings(akkaSystem: ActorSystem): AkkaCommitterSettings =
    AkkaCommitterSettings(akkaSystem)

  def akkaChannel(akkaSystem: ActorSystem)(implicit
    concurrentEffect: ConcurrentEffect[F],
    contextShift: ContextShift[F]): KafkaChannels.AkkaChannel[F, K, V] =
    new KafkaChannels.AkkaChannel[F, K, V](
      topicName,
      akkaProducerSettings(akkaSystem),
      akkaConsumerSettings(akkaSystem),
      akkaCommitterSettings(akkaSystem),
      akkaSystem)

  def kafkaStream: KafkaChannels.StreamingChannel[K, V] =
    new KafkaChannels.StreamingChannel[K, V](topic.topicDef.topicName, codec.keySerde, codec.valSerde)

  // schema registry operations
  def schemaRegister(implicit F: Sync[F]): F[(Option[Int], Option[Int])] =
    new SchemaRegistryApi[F](context.settings.schemaRegistrySettings)
      .register(topicName, topicDef.schemaForKey.schema, topicDef.schemaForVal.schema)

  def schemaDelete(implicit F: Sync[F]): F[(List[Integer], List[Integer])] =
    new SchemaRegistryApi[F](context.settings.schemaRegistrySettings).delete(topicName)

  def schemaCompatibility(implicit F: Sync[F]): F[CompatibilityTestReport] =
    new SchemaRegistryApi[F](context.settings.schemaRegistrySettings)
      .testCompatibility(topicName, topicDef.schemaForKey.schema, topicDef.schemaForVal.schema)

}
