package com.github.chenharryhua.nanjin.kafka

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import cats.{Eval, Show}
import cats.data.Reader
import cats.effect.concurrent.MVar
import cats.effect.{ConcurrentEffect, ContextShift, IO, Timer}
import com.github.chenharryhua.nanjin.codec.{KeySerde, SerdeOf, ValueSerde}
import fs2.Stream
import fs2.kafka.{KafkaByteConsumer, KafkaByteProducer}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.scala.StreamsBuilder

sealed abstract class KafkaContext[F[_]: ContextShift: Timer: ConcurrentEffect](
  settings: KafkaSettings) {

  protected lazy val akkaSystem: ActorSystem         = ActorSystem("nanjin")
  protected lazy val materializer: ActorMaterializer = ActorMaterializer.create(akkaSystem)

  final def asKey[K: SerdeOf]: KeySerde[K] =
    SerdeOf[K].asKey(settings.schemaRegistrySettings.props)
  final def asValue[V: SerdeOf]: ValueSerde[V] =
    SerdeOf[V].asValue(settings.schemaRegistrySettings.props)

  final def topic[K, V](topicDef: TopicDef[K, V]): KafkaTopic[F, K, V] =
    new KafkaTopic[F, K, V](
      topicDef,
      settings.schemaRegistrySettings,
      settings.consumerSettings,
      settings.producerSettings,
      sharedConsumer,
      sharedProducer,
      Eval.later(materializer))

  final def topic[K: SerdeOf: Show, V: SerdeOf: Show](topicName: String): KafkaTopic[F, K, V] =
    topic[K, V](TopicDef[K, V](topicName))

  final private[this] val sharedConsumer: Eval[MVar[F, KafkaByteConsumer]] =
    Eval.later {
      val consumerClient: KafkaConsumer[Array[Byte], Array[Byte]] =
        new KafkaConsumer[Array[Byte], Array[Byte]](
          settings.consumerSettings.sharedConsumerSettings,
          new ByteArrayDeserializer,
          new ByteArrayDeserializer)
      ConcurrentEffect[F].toIO(MVar.of[F, KafkaByteConsumer](consumerClient)).unsafeRunSync()
    }

  final private[this] val sharedProducer: Eval[KafkaByteProducer] =
    Eval.later {
      new KafkaProducer[Array[Byte], Array[Byte]](
        settings.producerSettings.sharedProducerSettings,
        new ByteArraySerializer,
        new ByteArraySerializer)
    }

  final def show: String = settings.show

  final def kafkaStreams[A](topology: Reader[StreamsBuilder, A]): Stream[F, KafkaStreams] =
    new KafkaStreamRunner[F](settings.streamSettings).stream[A](topology)
}

final class IoKafkaContext(settings: KafkaSettings)(
  implicit contextShift: ContextShift[IO],
  timer: Timer[IO])
    extends KafkaContext[IO](settings) {}

final class ZioKafkaContext(settings: KafkaSettings)(
  implicit contextShift: ContextShift[zio.Task],
  timer: Timer[zio.Task],
  ce: ConcurrentEffect[zio.Task]
) extends KafkaContext[zio.Task](settings) {}

final class MonixKafkaContext(settings: KafkaSettings) {}
