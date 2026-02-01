package com.github.chenharryhua.nanjin.kafka

import cats.Endo
import cats.effect.Resource
import cats.effect.kernel.{Async, Sync}
import cats.implicits.{catsSyntaxApplicativeError, toFlatMapOps, toFunctorOps, toTraverseOps}
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

/** Context for Kafka operations including producers, consumers, schema registry, Kafka Streams, and
  * administrative tasks.
  *
  * @param settings
  *   KafkaSettings containing configuration for producers, consumers, schema registry, streams, and admin.
  * @tparam F
  *   Effect type
  */
final class KafkaContext[F[_]] private (val settings: KafkaSettings)
    extends UpdateConfig[KafkaSettings, KafkaContext[F]] {

  /** Returns a new KafkaContext with updated settings. */
  override def updateConfig(f: Endo[KafkaSettings]): KafkaContext[F] =
    new KafkaContext[F](f(settings))

  // --------------------------------------------------------------------------
  // State Stores and Serdes
  // --------------------------------------------------------------------------

  /** Create state stores for the given topic.
    *
    * @param topic
    *   Topic with schema information
    * @tparam K
    *   Key type
    * @tparam V
    *   Value type
    * @return
    *   StateStores instance
    */
  def store[K, V](topic: KafkaTopic[K, V]): StateStores[K, V] =
    StateStores[K, V](topic.register(settings.schemaRegistrySettings))

  /** Returns the registered Serde pair for a topic.
    *
    * @param topic
    *   Topic to register Serde for
    * @tparam K
    *   Key type
    * @tparam V
    *   Value type
    */
  def serde[K, V](topic: KafkaTopic[K, V]): TopicSerde[K, V] =
    topic.register(settings.schemaRegistrySettings)

  /** Returns the Kafka Serde for a key based on unregistered Serde and schema registry. */
  def asKey[A](rs: UnregisteredSerde[A]): Serde[A] =
    rs.asKey(settings.schemaRegistrySettings.config).serde

  /** Returns the Kafka Serde for a value based on unregistered Serde and schema registry. */
  def asValue[A](rs: UnregisteredSerde[A]): Serde[A] =
    rs.asValue(settings.schemaRegistrySettings.config).serde

  // --------------------------------------------------------------------------
  // Schema Registry
  // --------------------------------------------------------------------------

  /** Returns a SchemaRegistryApi for interacting with the configured Schema Registry.
    *
    * @note
    *   Throws [[IllegalStateException]] if the URL config is absent.
    */
  def schemaRegistry(implicit F: Sync[F]): SchemaRegistryApi[F] = {
    val url_config = AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG
    val url: String =
      settings.schemaRegistrySettings.config
        .getOrElse(url_config, throw new IllegalStateException(s"Fatal error: $url_config is absent"))

    val cacheCapacity: Int = settings.schemaRegistrySettings.config
      .get(AbstractKafkaSchemaSerDeConfig.MAX_SCHEMAS_PER_SUBJECT_CONFIG)
      .flatMap(s => Try(s.toInt).toOption)
      .getOrElse(AbstractKafkaSchemaSerDeConfig.MAX_SCHEMAS_PER_SUBJECT_DEFAULT)

    new SchemaRegistryApiImpl[F](new CachedSchemaRegistryClient(url, cacheCapacity))
  }

  /** Check if the local topic schemas are backward compatible with broker schemas.
    *
    * @param topic
    *   Topic to check
    * @return
    *   Effect producing `true` if backward compatible, `false` otherwise
    */
  def isCompatible[K, V](topic: KafkaTopic[K, V])(implicit F: Sync[F]): F[Boolean] =
    topic match {
      case topic @ AvroTopic(_, pair) =>
        schemaRegistry.fetchOptionalAvroSchema(topic).map(pair.optionalSchemaPair.isBackwardCompatible)
      case topic @ ProtoTopic(_, pair) =>
        schemaRegistry.fetchOptionalProtobufSchema(topic).map(pair.optionalSchemaPair.isBackwardCompatible)
      case topic @ JsonTopic(_, pair) =>
        schemaRegistry.fetchOptionalJsonSchema(topic).map(pair.optionalSchemaPair.isBackwardCompatible)
    }

  // --------------------------------------------------------------------------
  // Consumers
  // --------------------------------------------------------------------------

  /** Create a typed consumer for the topic.
    *
    * @param topic
    *   KafkaTopic to consume
    * @return
    *   ConsumeKafka instance
    */
  def consume[K, V](topic: KafkaTopic[K, V])(implicit F: Async[F]): ConsumeKafka[F, K, V] =
    new ConsumeKafka[F, K, V](
      topic.topicName,
      topic.consumerSettings(settings.schemaRegistrySettings, settings.consumerSettings)
    )

  /** Create a raw byte consumer for the topic name.
    */
  def consumeBytes(topicName: TopicNameL)(implicit F: Async[F]): ConsumeBytes[F] =
    new ConsumeBytes[F](
      TopicName(topicName),
      ConsumerSettings[F, Array[Byte], Array[Byte]](
        Deserializer[F, Array[Byte]],
        Deserializer[F, Array[Byte]]).withProperties(settings.consumerSettings.properties)
    )

  /** Create a consumer that produces Avro GenericRecord values.
    *
    * @note
    *   May fetch schemas from Schema Registry on construction.
    */
  def consumeGenericRecord[K, V](avroTopic: AvroTopic[K, V])(implicit
    F: Async[F]): ConsumeGenericRecord[F, K, V] =
    new ConsumeGenericRecord[F, K, V](
      avroTopic,
      schemaRegistry.fetchOptionalAvroSchema(avroTopic),
      ConsumerSettings[F, Array[Byte], Array[Byte]](
        Deserializer[F, Array[Byte]],
        Deserializer[F, Array[Byte]]).withProperties(settings.consumerSettings.properties)
    )

  // --------------------------------------------------------------------------
  // Producers
  // --------------------------------------------------------------------------

  /** Create a typed producer for a topic.
    *
    * @note
    *   The returned producer checks schema compatibility before producing.
    */
  def produce[K, V](topic: KafkaTopic[K, V])(implicit F: Async[F]): ProduceKafka[F, K, V] =
    new ProduceKafka[F, K, V](
      topic.topicName,
      topic.producerSettings(settings.schemaRegistrySettings, settings.producerSettings),
      isCompatible(topic)
    )

  /** Shared producer for a SerdePair without a specific topic. */
  def sharedProduce[K, V](pair: SerdePair[K, V])(implicit F: Async[F]): ProduceShared[F, K, V] =
    new ProduceShared[F, K, V](
      pair.producerSettings(settings.schemaRegistrySettings, settings.producerSettings)
    )

  /** Produce Avro GenericRecord values.
    *
    * @note
    *   May fetch schema from Schema Registry.
    */
  def produceGenericRecord[K, V](avroTopic: AvroTopic[K, V])(implicit
    F: Async[F]): ProduceGenericRecord[F, K, V] =
    new ProduceGenericRecord[F, K, V](
      avroTopic,
      schemaRegistry.fetchOptionalAvroSchema(avroTopic),
      settings.schemaRegistrySettings,
      ProducerSettings[F, Array[Byte], Array[Byte]](Serializer[F, Array[Byte]], Serializer[F, Array[Byte]])
        .withProperties(settings.producerSettings.properties)
    )

  // --------------------------------------------------------------------------
  // Kafka Streams
  // --------------------------------------------------------------------------

  /** Build a Kafka Streams topology.
    *
    * @param applicationId
    *   Kafka Streams application ID
    * @param topology
    *   Function to build the topology, receives a [[StreamsBuilder]] and [[StreamsSerde]].
    */
  def buildStreams(applicationId: String)(topology: (StreamsBuilder, StreamsSerde) => Unit)(implicit
    F: Async[F]): KafkaStreamsBuilder[F] =
    streaming.KafkaStreamsBuilder[F](
      applicationId,
      settings.streamSettings,
      settings.schemaRegistrySettings,
      topology)

  // --------------------------------------------------------------------------
  // Admin
  // --------------------------------------------------------------------------

  /** Resource for a KafkaAdminClient. */
  def admin(implicit F: Async[F]): Resource[F, KafkaAdminClient[F]] =
    KafkaAdminClient.resource[F](settings.adminSettings)

  /** Resource for a KafkaAdminApi targeting a specific topic. */
  def admin(topicName: TopicNameL)(implicit F: Async[F]): Resource[F, KafkaAdminApi[F]] =
    KafkaAdminApi[F](admin, TopicName(topicName), settings.consumerSettings)

  /** Remove consumer group offsets for all topics except those in `keeps`.
    *
    * @param groupId
    *   Consumer group ID
    * @param keeps
    *   List of topics to preserve
    * @return
    *   List of topics successfully removed from the consumer group
    *
    * @note
    *   Errors during deletion are ignored (logged via `attempt`).
    */
  def ungroup(groupId: String, keeps: List[TopicName] = Nil)(implicit F: Async[F]): F[List[TopicName]] =
    admin
      .use(
        _.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata
          .map(_.keys.map(_.topic()).toList.distinct.diff(keeps.map(_.value))))
      .flatMap(
        _.traverse { tn =>
          admin(TopicName.unsafeFrom(tn).name).use(_.deleteConsumerGroupOffsets(groupId)).attempt.map {
            case Left(_)  => None
            case Right(_) => Some(TopicName.unsafeFrom(tn))
          }
        }.map(_.flatten)
      )
}

object KafkaContext {

  /** Construct a KafkaContext from KafkaSettings. */
  def apply[F[_]](settings: KafkaSettings): KafkaContext[F] = new KafkaContext[F](settings)
}
