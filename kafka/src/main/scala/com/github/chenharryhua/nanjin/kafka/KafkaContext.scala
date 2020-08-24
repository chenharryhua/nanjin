package com.github.chenharryhua.nanjin.kafka

import cats.data.Reader
import cats.effect.{ConcurrentEffect, IO, Sync}
import com.github.chenharryhua.nanjin.messages.kafka.codec.SerdeOf
import fs2.Stream
import monix.eval.{Task => MTask}
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.scala.StreamsBuilder
import zio.{Task => ZTask}

sealed abstract class KafkaContext[F[_]](val settings: KafkaSettings) extends Serializable {

  final def updateSettings(f: KafkaSettings => KafkaSettings): KafkaContext[F] =
    new KafkaContext[F](f(settings)) {}

  final def asKey[K: SerdeOf]: Serde[K] =
    SerdeOf[K].asKey(settings.schemaRegistrySettings.config).serde

  final def asValue[V: SerdeOf]: Serde[V] =
    SerdeOf[V].asValue(settings.schemaRegistrySettings.config).serde

  final def topic[K, V](topicDef: TopicDef[K, V]): KafkaTopic[F, K, V] =
    new KafkaTopic[F, K, V](topicDef, this)

  final def topic[K: SerdeOf, V: SerdeOf](topicName: String): KafkaTopic[F, K, V] =
    topic[K, V](TopicDef[K, V](TopicName.unsafeFrom(topicName)))

  final def runStreams(topology: Reader[StreamsBuilder, Unit])(implicit
    ce: ConcurrentEffect[F]): Stream[F, KafkaStreams] =
    new KafkaStreamRunner[F](settings.streamSettings).stream(topology)

  final def genCaseClass(topicName: TopicName)(implicit F: Sync[F]): F[String] =
    new SchemaRegistryApi[F](settings.schemaRegistrySettings).genCaseClass(topicName)
}

final class IoKafkaContext(settings: KafkaSettings) extends KafkaContext[IO](settings)
final class ZioKafkaContext(settings: KafkaSettings) extends KafkaContext[ZTask](settings)
final class MonixKafkaContext(settings: KafkaSettings) extends KafkaContext[MTask](settings)
