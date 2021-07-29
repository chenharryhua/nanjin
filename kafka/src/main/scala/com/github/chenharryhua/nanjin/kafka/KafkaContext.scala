package com.github.chenharryhua.nanjin.kafka

import cats.data.Reader
import cats.effect.IO
import cats.effect.kernel.Sync
import cats.syntax.functor.*
import cats.syntax.show.*
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.messages.kafka.codec.SerdeOf
import monix.eval.Task as MTask
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.StreamsBuilder
import zio.Task as ZTask

sealed abstract class KafkaContext[F[_]](val settings: KafkaSettings) extends Serializable {

  final def updateSettings(f: KafkaSettings => KafkaSettings): KafkaContext[F] =
    new KafkaContext[F](f(settings)) {}

  final def withGroupId(groupId: String): KafkaContext[F]     = updateSettings(_.withGroupId(groupId))
  final def withApplicationId(appId: String): KafkaContext[F] = updateSettings(_.withApplicationId(appId))

  final def asKey[K: SerdeOf]: Serde[K]   = SerdeOf[K].asKey(settings.schemaRegistrySettings.config).serde
  final def asValue[V: SerdeOf]: Serde[V] = SerdeOf[V].asValue(settings.schemaRegistrySettings.config).serde

  final def topic[K, V](topicDef: TopicDef[K, V]): KafkaTopic[F, K, V] = new KafkaTopic[F, K, V](topicDef, this)

  final def topic[K: SerdeOf, V: SerdeOf](topicName: String): KafkaTopic[F, K, V] =
    topic[K, V](TopicDef[K, V](TopicName.unsafeFrom(topicName)))

  final def buildStreams(topology: Reader[StreamsBuilder, Unit]): KafkaStreamsBuilder[F] =
    new KafkaStreamsBuilder[F](settings.streamSettings, topology, Nil)

  final def schema(topicName: String)(implicit F: Sync[F]): F[String] =
    new SchemaRegistryApi[F](settings.schemaRegistrySettings).kvSchema(TopicName.unsafeFrom(topicName)).map(_.show)
}

private[kafka] object KafkaContext {
  def ioContext(settings: KafkaSettings): KafkaContext[IO]       = new KafkaContext[IO](settings) {}
  def zioContext(settings: KafkaSettings): KafkaContext[ZTask]   = new KafkaContext[ZTask](settings) {}
  def monixContext(settings: KafkaSettings): KafkaContext[MTask] = new KafkaContext[MTask](settings) {}
}
