package com.github.chenharryhua.nanjin.kafka

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import cats.data.Reader
import cats.effect.concurrent.MVar
import cats.effect.{ConcurrentEffect, ContextShift, IO, Resource, Timer}
import cats.{Eval, Show}
import com.github.chenharryhua.nanjin.codec.SerdeOf
import fs2.Stream
import fs2.kafka._
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.scala.StreamsBuilder
import com.github.chenharryhua.nanjin.codec.KafkaSerde

sealed abstract class KafkaContext[F[_]: ContextShift: Timer: ConcurrentEffect](
  settings: KafkaSettings) {

  protected lazy val akkaSystem: ActorSystem         = ActorSystem("nanjin")
  protected lazy val materializer: ActorMaterializer = ActorMaterializer.create(akkaSystem)

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

  final private[this] val adminClientSettings: AdminClientSettings[F] =
    AdminClientSettings[F].withProperties(settings.sharedAdminSettings.props)

  final val admin: Resource[F, KafkaAdminClient[F]] =
    adminClientResource(adminClientSettings)

  final def asKey[K: SerdeOf]: KafkaSerde.Key[K] =
    SerdeOf[K].asKey(settings.schemaRegistrySettings.props)

  final def asValue[V: SerdeOf]: KafkaSerde.Value[V] =
    SerdeOf[V].asValue(settings.schemaRegistrySettings.props)

  final def topic[K, V](topicDef: TopicDef[K, V]): KafkaTopic[F, K, V] =
    KafkaTopic[F, K, V](
      topicDef,
      settings.schemaRegistrySettings,
      settings.consumerSettings,
      settings.producerSettings,
      adminClientSettings,
      sharedConsumer,
      sharedProducer,
      Eval.later(materializer),
      SparkafkaConf.default
    )

  final def topic[K: SerdeOf: Show, V: SerdeOf: Show](topicName: String): KafkaTopic[F, K, V] =
    topic[K, V](TopicDef[K, V](topicName))

  final def kafkaStreams(topology: Reader[StreamsBuilder, Unit]): Stream[F, KafkaStreams] =
    new KafkaStreamRunner[F](settings.streamSettings).stream(topology)

}

final class IoKafkaContext(settings: KafkaSettings)(implicit cs: ContextShift[IO], timer: Timer[IO])
    extends KafkaContext[IO](settings) {}

final class ZioKafkaContext(settings: KafkaSettings)(
  implicit cs: ContextShift[zio.Task],
  timer: Timer[zio.Task],
  ce: ConcurrentEffect[zio.Task]
) extends KafkaContext[zio.Task](settings) {}

final class MonixKafkaContext(settings: KafkaSettings)(
  implicit cs: ContextShift[monix.eval.Task],
  timer: Timer[monix.eval.Task],
  ce: ConcurrentEffect[monix.eval.Task])
    extends KafkaContext[monix.eval.Task](settings) {}
