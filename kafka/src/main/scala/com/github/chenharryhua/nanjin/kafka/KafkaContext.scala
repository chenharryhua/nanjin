package com.github.chenharryhua.nanjin.kafka

import cats.effect.concurrent.MVar
import cats.effect.{ConcurrentEffect, ContextShift, Timer}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.ByteArrayDeserializer

final class KafkaContext[F[_]: ContextShift: Timer: ConcurrentEffect](val settings: KafkaSettings)
    extends SerdeModule(settings.schemaRegistrySettings) {

  def asKey[K: SerdeOf]: KeySerde[K]     = SerdeOf[K].asKey(Map.empty)
  def asValue[V: SerdeOf]: ValueSerde[V] = SerdeOf[V].asValue(Map.empty)

  def topic[K: SerdeOf, V: SerdeOf](
    topicName: KafkaTopicName
  ): KafkaTopic[K, V] =
    new KafkaTopic[K, V](
      topicName,
      settings.fs2Settings,
      settings.akkaSettings,
      settings.schemaRegistrySettings,
      asKey[K],
      asValue[V]
    )

  private[this] val sharedConsumer: MVar[F, KafkaConsumer[Array[Byte], Array[Byte]]] = {
    val consumerClient: KafkaConsumer[Array[Byte], Array[Byte]] =
      new KafkaConsumer[Array[Byte], Array[Byte]](
        settings.sharedConsumerSettings.settings,
        new ByteArrayDeserializer,
        new ByteArrayDeserializer)
    ConcurrentEffect[F]
      .toIO(MVar.of[F, KafkaConsumer[Array[Byte], Array[Byte]]](consumerClient))
      .unsafeRunSync()
  }
}
