package com.github.chenharryhua.nanjin.kafka

import cats.Endo
import cats.effect.Resource
import cats.effect.kernel.{Async, Sync}
import cats.implicits.toFunctorOps
import com.github.chenharryhua.nanjin.common.UpdateConfig
import com.github.chenharryhua.nanjin.common.kafka.{TopicName, TopicNameL}
import com.github.chenharryhua.nanjin.kafka.connector.*
import com.github.chenharryhua.nanjin.kafka.streaming.{KafkaStreamsBuilder, StateStores, StreamsSerde}
import com.github.chenharryhua.nanjin.messages.kafka.codec.UnregisteredSerde
import fs2.kafka.*
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.StreamsBuilder

import scala.util.Try

final class KafkaContext[F[_]] private (val settings: KafkaSettings)
    extends UpdateConfig[KafkaSettings, KafkaContext[F]] {

  override def updateConfig(f: Endo[KafkaSettings]): KafkaContext[F] =
    new KafkaContext[F](f(settings))

  def store[K, V](topic: KafkaTopic[K, V]): StateStores[K, V] =
    StateStores[K, V](topic.register(settings.schemaRegistrySettings))

  /*
   * serde
   */
  def serde[K, V](topic: KafkaTopic[K, V]): TopicSerde[K, V] =
    topic.register(settings.schemaRegistrySettings)

  def asKey[A](rs: UnregisteredSerde[A]): Serde[A] =
    rs.asKey(settings.schemaRegistrySettings.config).serde

  def asValue[A](rs: UnregisteredSerde[A]): Serde[A] =
    rs.asValue(settings.schemaRegistrySettings.config).serde

  /*
   * schema registry
   */

  def schemaRegistry(implicit F: Sync[F]): SchemaRegistryApi[F] = {
    val url_config = AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG
    val url = settings.schemaRegistrySettings.config.get(url_config) match {
      case Some(value) => value
      case None        => throw new Exception(s"$url_config is absent")
    }
    val cacheCapacity: Int = settings.schemaRegistrySettings.config
      .get(AbstractKafkaSchemaSerDeConfig.MAX_SCHEMAS_PER_SUBJECT_CONFIG)
      .flatMap(s => Try(s.toInt).toOption)
      .getOrElse(AbstractKafkaSchemaSerDeConfig.MAX_SCHEMAS_PER_SUBJECT_DEFAULT)
    new SchemaRegistryApiImpl[F](new CachedSchemaRegistryClient(url, cacheCapacity))
  }

  def isCompatible[K, V](topic: KafkaTopic[K, V])(implicit F: Sync[F]): F[Boolean] =
    topic match {
      case topic @ AvroTopic(_, pair) =>
        schemaRegistry.fetchOptionalAvroSchema(topic).map(pair.optionalSchemaPair.isBackwardCompatible)
      case topic @ ProtoTopic(_, pair) =>
        schemaRegistry.fetchOptionalProtobufSchema(topic).map(pair.optionalSchemaPair.isBackwardCompatible)
      case topic @ JsonTopic(_, pair) =>
        schemaRegistry.fetchOptionalJsonSchema(topic).map(pair.optionalSchemaPair.isBackwardCompatible)
    }

  /*
   * consumer
   */

  def consume[K, V](topic: KafkaTopic[K, V])(implicit F: Async[F]): ConsumeKafka[F, K, V] =
    new ConsumeKafka[F, K, V](
      topic.topicName,
      topic.consumerSettings(settings.schemaRegistrySettings, settings.consumerSettings)
    )

  def consumeBytes(topicName: TopicNameL)(implicit F: Async[F]): ConsumeBytes[F] =
    new ConsumeBytes[F](
      TopicName(topicName),
      ConsumerSettings[F, Array[Byte], Array[Byte]](
        Deserializer[F, Array[Byte]],
        Deserializer[F, Array[Byte]]).withProperties(settings.consumerSettings.properties)
    )

  def consumeGenericRecord[K, V](avroTopic: AvroTopic[K, V])(implicit
    F: Async[F]): ConsumeGenericRecord[F, K, V] =
    new ConsumeGenericRecord[F, K, V](
      avroTopic,
      schemaRegistry.fetchOptionalAvroSchema(avroTopic),
      ConsumerSettings[F, Array[Byte], Array[Byte]](
        Deserializer[F, Array[Byte]],
        Deserializer[F, Array[Byte]]).withProperties(settings.consumerSettings.properties)
    )

  /*
   * producer
   */

  def produce[K, V](topic: KafkaTopic[K, V])(implicit F: Async[F]): ProduceKafka[F, K, V] =
    new ProduceKafka[F, K, V](
      topic.topicName,
      topic.producerSettings(settings.schemaRegistrySettings, settings.producerSettings),
      isCompatible(topic)
    )

  def sharedProduce[K, V](pair: SerdePair[K, V])(implicit F: Async[F]): ProduceShared[F, K, V] =
    new ProduceShared[F, K, V](
      pair.producerSettings(settings.schemaRegistrySettings, settings.producerSettings)
    )

  def produceGenericRecord[K, V](avroTopic: AvroTopic[K, V])(implicit
    F: Async[F]): ProduceGenericRecord[F, K, V] =
    new ProduceGenericRecord[F, K, V](
      avroTopic,
      schemaRegistry.fetchOptionalAvroSchema(avroTopic),
      settings.schemaRegistrySettings,
      ProducerSettings[F, Array[Byte], Array[Byte]](Serializer[F, Array[Byte]], Serializer[F, Array[Byte]])
        .withProperties(settings.producerSettings.properties)
    )

  /*
   * kafka streaming
   */

  def buildStreams(applicationId: String)(topology: (StreamsBuilder, StreamsSerde) => Unit)(implicit
    F: Async[F]): KafkaStreamsBuilder[F] =
    streaming.KafkaStreamsBuilder[F](
      applicationId,
      settings.streamSettings,
      settings.schemaRegistrySettings,
      topology)

  /*
   * admin topic
   */

  def admin(implicit F: Async[F]): Resource[F, KafkaAdminClient[F]] =
    KafkaAdminClient.resource[F](settings.adminSettings)

  def admin(topicName: TopicNameL)(implicit F: Async[F]): Resource[F, KafkaAdminApi[F]] =
    KafkaAdminApi[F](admin, TopicName(topicName), settings.consumerSettings)
}

object KafkaContext {
  def apply[F[_]](settings: KafkaSettings): KafkaContext[F] = new KafkaContext[F](settings)
}
