package com.github.chenharryhua.nanjin.kafka

import cats.data.Reader
import cats.effect.{ConcurrentEffect, ContextShift, IO, Timer}
import com.github.chenharryhua.nanjin.kafka.codec.{NJSerde, SerdeOf}
import fs2.Stream
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.scala.StreamsBuilder

sealed class KafkaContext[F[_]](val settings: KafkaSettings)(
  implicit
  val timer: Timer[F],
  val contextShift: ContextShift[F],
  val concurrentEffect: ConcurrentEffect[F]) {

  final def asKey[K: SerdeOf]: NJSerde[K] =
    SerdeOf[K].asKey(settings.schemaRegistrySettings.config)

  final def asValue[V: SerdeOf]: NJSerde[V] =
    SerdeOf[V].asValue(settings.schemaRegistrySettings.config)

  final def topic[K, V](topicDef: TopicDef[K, V]): KafkaTopic[F, K, V] =
    new KafkaTopic[F, K, V](topicDef, settings)

  final def topic[K: SerdeOf, V: SerdeOf](topicName: TopicName): KafkaTopic[F, K, V] =
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
