package com.github.chenharryhua.nanjin.kafka

import cats.data.Reader
import cats.effect.IO
import cats.effect.kernel.{Async, Sync}
import cats.syntax.functor.*
import cats.syntax.show.*
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.streaming.{KafkaStreamsBuilder, NJStateStore}
import com.github.chenharryhua.nanjin.messages.kafka.codec.{AvroCodec, KJson, SerdeOf}
import io.circe.Json
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

  final def asKey[K](avro: AvroCodec[K]): Serde[K] =
    SerdeOf[K](avro).asKey(settings.schemaRegistrySettings.config).serde
  final def asValue[V](avro: AvroCodec[V]): Serde[V] =
    SerdeOf[V](avro).asValue(settings.schemaRegistrySettings.config).serde

  final def topic[K, V](topicDef: TopicDef[K, V]): KafkaTopic[F, K, V] = new KafkaTopic[F, K, V](topicDef, this)

  final def topic[K: SerdeOf, V: SerdeOf](topicName: String): KafkaTopic[F, K, V] =
    topic[K, V](TopicDef[K, V](TopicName.unsafeFrom(topicName)))

  final def jsonTopic(topicName: String): KafkaTopic[F, KJson[Json], KJson[Json]] =
    topic(TopicDef[KJson[Json], KJson[Json]](TopicName.unsafeFrom(topicName)))

  final def byteTopic(topicName: String): KafkaTopic[F, Array[Byte], Array[Byte]] =
    topic(TopicDef[Array[Byte], Array[Byte]](TopicName.unsafeFrom(topicName)))

  final def stringTopic(topicName: String): KafkaTopic[F, String, String] =
    topic(TopicDef[String, String](TopicName.unsafeFrom(topicName)))

  final def store[K: SerdeOf, V: SerdeOf](storeName: String): NJStateStore[K, V] =
    NJStateStore[K, V](storeName, settings.schemaRegistrySettings, RawKeyValueSerdePair[K, V](SerdeOf[K], SerdeOf[V]))

  final def buildStreams(topology: Reader[StreamsBuilder, Unit])(implicit F: Async[F]): KafkaStreamsBuilder[F] =
    streaming.KafkaStreamsBuilder[F](settings.streamSettings, topology)

  final def schema(topicName: String)(implicit F: Sync[F]): F[String] =
    new SchemaRegistryApi[F](settings.schemaRegistrySettings).kvSchema(TopicName.unsafeFrom(topicName)).map(_.show)
}

private[kafka] object KafkaContext {
  def ioContext(settings: KafkaSettings): KafkaContext[IO]       = new KafkaContext[IO](settings) {}
  def zioContext(settings: KafkaSettings): KafkaContext[ZTask]   = new KafkaContext[ZTask](settings) {}
  def monixContext(settings: KafkaSettings): KafkaContext[MTask] = new KafkaContext[MTask](settings) {}
}
