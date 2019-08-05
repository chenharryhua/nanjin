package com.github.chenharryhua.nanjin.kafka

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import cats.Eval
import cats.effect.concurrent.MVar
import cats.effect.{ConcurrentEffect, ContextShift, IO, Timer}
import fs2.kafka.{KafkaByteConsumer, KafkaByteProducer}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}

sealed abstract class KafkaContext[F[_]: ContextShift: Timer: ConcurrentEffect](
  settings: KafkaSettings)
    extends SerdeModule(settings.schemaRegistrySettings) {

  protected val akkaSystem: ActorSystem        = ActorSystem("nanjin")
  implicit val materializer: ActorMaterializer = ActorMaterializer.create(akkaSystem)

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
      asKey[K],
      asValue[V])

  def topic[K: SerdeOf, V: SerdeOf](topicName: String): KafkaTopic[F, K, V] =
    topic[K, V](TopicDef[K, V](topicName))

  private[this] val sharedConsumer: Eval[MVar[F, KafkaByteConsumer]] =
    Eval.later {
      val consumerClient: KafkaConsumer[Array[Byte], Array[Byte]] =
        new KafkaConsumer[Array[Byte], Array[Byte]](
          settings.sharedConsumerSettings.settings,
          new ByteArrayDeserializer,
          new ByteArrayDeserializer)
      ConcurrentEffect[F].toIO(MVar.of[F, KafkaByteConsumer](consumerClient)).unsafeRunSync()
    }

  private[this] val sharedProducer: Eval[KafkaByteProducer] =
    Eval.later {
      new KafkaProducer[Array[Byte], Array[Byte]](
        settings.sharedProducerSettings.settings,
        new ByteArraySerializer,
        new ByteArraySerializer)
    }
}

final class IoKafkaContext(settings: KafkaSettings)(
  implicit contextShift: ContextShift[IO],
  timer: Timer[IO])
    extends KafkaContext[IO](settings) {
  def show: String = settings.show
}

final class ZioKafkaContext(settings: KafkaSettings)(
  implicit contextShift: ContextShift[zio.Task],
  timer: Timer[zio.Task],
  ce: ConcurrentEffect[zio.Task]
) extends KafkaContext[zio.Task](settings) {}

final class MonixKafkaContext(settings: KafkaSettings) {}
