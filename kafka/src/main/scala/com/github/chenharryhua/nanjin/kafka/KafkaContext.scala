package com.github.chenharryhua.nanjin.kafka

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import cats.Eval
import cats.effect.concurrent.MVar
import cats.effect.{ConcurrentEffect, ContextShift, IO, Timer}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}

sealed abstract class KafkaContext[F[_]: ContextShift: Timer: ConcurrentEffect](
  settings: KafkaSettings)
    extends SerdeModule(settings.schemaRegistrySettings) {

  protected val akkaSystem: Eval[ActorSystem]         = Eval.later(ActorSystem("nanjin"))
  protected val materializer: Eval[ActorMaterializer] = akkaSystem.map(ActorMaterializer.create)

  def asKey[K: SerdeOf]: KeySerde[K]     = SerdeOf[K].asKey(Map.empty)
  def asValue[V: SerdeOf]: ValueSerde[V] = SerdeOf[V].asValue(Map.empty)

  def topic[K: SerdeOf, V: SerdeOf](topicDef: TopicDef[K, V]): KafkaTopic[F, K, V] =
    new KafkaTopic[F, K, V](
      topicDef,
      settings.fs2Settings,
      settings.akkaSettings,
      settings.schemaRegistrySettings,
      sharedConsumer,
      sharedProducer,
      materializer,
      asKey[K],
      asValue[V])

  def topic[K: SerdeOf, V: SerdeOf](topicName: String): KafkaTopic[F, K, V] =
    topic[K, V](TopicDef[K, V](topicName))

  private[this] val sharedConsumer: Eval[MVar[F, KafkaConsumer[Array[Byte], Array[Byte]]]] =
    Eval.later {
      val consumerClient: KafkaConsumer[Array[Byte], Array[Byte]] =
        new KafkaConsumer[Array[Byte], Array[Byte]](
          settings.sharedConsumerSettings.settings,
          new ByteArrayDeserializer,
          new ByteArrayDeserializer)
      ConcurrentEffect[F]
        .toIO(MVar.of[F, KafkaConsumer[Array[Byte], Array[Byte]]](consumerClient))
        .unsafeRunSync()
    }

  private[this] val sharedProducer: Eval[KafkaProducer[Array[Byte], Array[Byte]]] =
    Eval.later(
      new KafkaProducer[Array[Byte], Array[Byte]](
        settings.sharedProducerSettings.settings,
        new ByteArraySerializer,
        new ByteArraySerializer))
}

final class IoKafkaContext(settings: KafkaSettings)(
  implicit contextShift: ContextShift[IO],
  timer: Timer[IO])
    extends KafkaContext[IO](settings) {}

final class ZioKafkaContext(settings: KafkaSettings) {}

final class MonixKafkaContext(settings: KafkaSettings) {}
