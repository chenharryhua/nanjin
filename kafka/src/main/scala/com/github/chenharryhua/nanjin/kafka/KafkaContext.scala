package com.github.chenharryhua.nanjin.kafka

import cats.data.Reader
import cats.effect.kernel.{Async, Sync}
import cats.syntax.functor.*
import cats.syntax.show.*
import com.github.chenharryhua.nanjin.common.kafka.{StoreName, TopicName}
import com.github.chenharryhua.nanjin.kafka.streaming.{KafkaStreamsBuilder, NJStateStore}
import com.github.chenharryhua.nanjin.messages.kafka.codec.{NJAvroCodec, SerdeOf}
import fs2.kafka.{ConsumerSettings, Deserializer}
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.StreamsBuilder

final class KafkaContext[F[_]](val settings: KafkaSettings) extends Serializable {

  def updateSettings(f: KafkaSettings => KafkaSettings): KafkaContext[F] =
    new KafkaContext[F](f(settings))

  def withGroupId(groupId: String): KafkaContext[F]     = updateSettings(_.withGroupId(groupId))
  def withApplicationId(appId: String): KafkaContext[F] = updateSettings(_.withApplicationId(appId))

  def asKey[K: SerdeOf]: Serde[K]   = SerdeOf[K].asKey(settings.schemaRegistrySettings.config).serde
  def asValue[V: SerdeOf]: Serde[V] = SerdeOf[V].asValue(settings.schemaRegistrySettings.config).serde

  def asKey[K](avro: NJAvroCodec[K]): Serde[K] =
    SerdeOf[K](avro).asKey(settings.schemaRegistrySettings.config).serde
  def asValue[V](avro: NJAvroCodec[V]): Serde[V] =
    SerdeOf[V](avro).asValue(settings.schemaRegistrySettings.config).serde

  def topic[K, V](topicDef: TopicDef[K, V]): KafkaTopic[F, K, V] =
    new KafkaTopic[F, K, V](topicDef, this)

  def topic[K: SerdeOf, V: SerdeOf](topicName: TopicName): KafkaTopic[F, K, V] =
    topic[K, V](TopicDef[K, V](topicName))

  def consume(topicName: TopicName)(implicit F: Sync[F]): Fs2Consume[F] =
    new Fs2Consume[F](
      topicName,
      ConsumerSettings[F, Array[Byte], Array[Byte]](
        Deserializer[F, Array[Byte]],
        Deserializer[F, Array[Byte]]).withProperties(settings.consumerSettings.config)
    )

  def store[K: SerdeOf, V: SerdeOf](storeName: StoreName): NJStateStore[K, V] =
    NJStateStore[K, V](
      storeName,
      settings.schemaRegistrySettings,
      RawKeyValueSerdePair[K, V](SerdeOf[K], SerdeOf[V]))

  def buildStreams(topology: Reader[StreamsBuilder, Unit])(implicit F: Async[F]): KafkaStreamsBuilder[F] =
    streaming.KafkaStreamsBuilder[F](settings.streamSettings, topology)

  def buildStreams(topology: StreamsBuilder => Unit)(implicit F: Async[F]): KafkaStreamsBuilder[F] =
    buildStreams(Reader(topology))

  def schema(topicName: String)(implicit F: Sync[F]): F[String] =
    new SchemaRegistryApi[F](settings.schemaRegistrySettings)
      .kvSchema(TopicName.unsafeFrom(topicName))
      .map(_.show)
}
