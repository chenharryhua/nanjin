package com.github.chenharryhua.nanjin.kafka

import cats.Endo
import cats.effect.Resource
import cats.effect.kernel.{Async, Sync}
import cats.syntax.applicativeError.catsSyntaxApplicativeError
import cats.syntax.flatMap.toFlatMapOps
import cats.syntax.functor.toFunctorOps
import cats.syntax.traverse.toTraverseOps
import com.github.chenharryhua.nanjin.common.UpdateConfig
import com.github.chenharryhua.nanjin.kafka.admins.{
  AdminTopic,
  AdminTopicGroup,
  SchemaRegistryApi,
  SnapshotConsumer
}
import com.github.chenharryhua.nanjin.kafka.connector.*
import com.github.chenharryhua.nanjin.kafka.streaming.{KafkaStreamsBuilder, StateStores, StreamsSerde}
import com.github.chenharryhua.nanjin.kafka.{
  makePureConsumer,
  AvroTopic,
  GroupId,
  JsonTopic,
  KafkaSettings,
  KafkaTopic,
  ProtoTopic,
  PureConsumerSettings,
  SerdePair,
  TopicName,
  TopicSerde
}
import com.github.chenharryhua.nanjin.messages.kafka.codec.UnregisteredSerde
import fs2.kafka.*
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.StreamsBuilder

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
    * @throws java.lang.IllegalStateException
    *   if the URL config is absent
    */
  def schemaRegistry(using F: Sync[F]): SchemaRegistryApi[F] = {
    val url_config = AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG
    val url: String =
      settings.schemaRegistrySettings.config.getOrElse(
        url_config,
        throw new IllegalStateException(s"Fatal error: $url_config is absent")
      ) // scalafix:ok

    val cacheCapacity: Int = settings.schemaRegistrySettings.config
      .get(AbstractKafkaSchemaSerDeConfig.MAX_SCHEMAS_PER_SUBJECT_CONFIG)
      .flatMap(s => Try(s.toInt).toOption)
      .getOrElse(AbstractKafkaSchemaSerDeConfig.MAX_SCHEMAS_PER_SUBJECT_DEFAULT)

    SchemaRegistryApi[F](new CachedSchemaRegistryClient(url, cacheCapacity))
  }

  /** Check if the local topic schemas are backward compatible with broker schemas.
    *
    * @param topic
    *   Topic to check
    * @return
    *   Effect producing `true` if backward compatible, `false` otherwise
    */
  def isCompatible[K, V](topic: KafkaTopic[K, V])(using F: Sync[F]): F[Boolean] =
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
  def consume[K, V](topic: KafkaTopic[K, V])(using F: Async[F]): ConsumeKafka[F, K, V] =
    new ConsumeKafka[F, K, V](
      topic.topicName,
      topic.consumerSettings(settings.schemaRegistrySettings, settings.consumerSettings)
    )

  private def byteConsumerSetting(using F: Sync[F]): ConsumerSettings[F, Array[Byte], Array[Byte]] =
    ConsumerSettings[F, Array[Byte], Array[Byte]](Deserializer[F, Array[Byte]], Deserializer[F, Array[Byte]])
      .withProperties(settings.consumerSettings.properties)

  /** Create a raw byte consumer for the topic name.
    */
  def consumeBytes(topicName: TopicName)(using F: Async[F]): ConsumeBytes[F] =
    new ConsumeBytes[F](topicName, byteConsumerSetting)

  /** Create a consumer that produces Avro GenericRecord values.
    *
    * @note
    *   May fetch schemas from Schema Registry on construction.
    */
  def consumeGenericRecord[K, V](avroTopic: AvroTopic[K, V])(using
    F: Async[F]): ConsumeGenericRecord[F, K, V] =
    new ConsumeGenericRecord[F, K, V](
      avroTopic,
      schemaRegistry.fetchOptionalAvroSchema(avroTopic),
      byteConsumerSetting)

  // --------------------------------------------------------------------------
  // Producers
  // --------------------------------------------------------------------------

  /** Create a typed producer for a topic.
    *
    * @note
    *   The returned producer checks schema compatibility before producing.
    */
  def produce[K, V](topic: KafkaTopic[K, V])(using F: Async[F]): ProduceKafka[F, K, V] =
    new ProduceKafka[F, K, V](
      topic.topicName,
      topic.producerSettings(settings.schemaRegistrySettings, settings.producerSettings),
      isCompatible(topic)
    )

  /** Shared producer for a SerdePair without a specific topic. */
  def sharedProduce[K, V](pair: SerdePair[K, V])(using F: Async[F]): ProduceShared[F, K, V] =
    new ProduceShared[F, K, V](
      pair.producerSettings(settings.schemaRegistrySettings, settings.producerSettings)
    )

  /** Produce Avro GenericRecord values.
    *
    * @note
    *   May fetch schema from Schema Registry.
    */
  def produceGenericRecord[K, V](avroTopic: AvroTopic[K, V])(using
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
    *   Function to build the topology, receives a `StreamsBuilder` and `StreamsSerde`.
    */
  def buildStreams(applicationId: String)(topology: (StreamsBuilder, StreamsSerde) => Unit)(using
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
  def admin(using F: Async[F]): Resource[F, KafkaAdminClient[F]] =
    KafkaAdminClient.resource[F](settings.adminSettings)

  def admin(topicName: TopicName, groupId: GroupId)(using F: Async[F]): Resource[F, AdminTopicGroup[F]] =
    for {
      admin <- KafkaAdminClient.resource[F](settings.adminSettings)
      consumer <- SnapshotConsumer(
        topicName,
        PureConsumerSettings
          .withProperties(settings.consumerSettings.properties)
          .withAutoOffsetReset(AutoOffsetReset.None)
          .withEnableAutoCommit(false)
          .withGroupId(groupId.value)
      )
    } yield AdminTopicGroup(admin, consumer, topicName, groupId)

  def admin(topicName: TopicName)(using F: Async[F]): Resource[F, AdminTopic[F]] =
    for {
      admin <- KafkaAdminClient.resource[F](settings.adminSettings)
      consumer <- SnapshotConsumer(
        topicName,
        PureConsumerSettings
          .withProperties(settings.consumerSettings.properties)
          .withAutoOffsetReset(AutoOffsetReset.None)
          .withEnableAutoCommit(false)
      )
    } yield AdminTopic(admin, consumer, topicName)

  /** Remove consumer group offsets for all topics except those in `keeps`.
    *
    * @param groupId
    *   Consumer group ID
    * @param keeps
    *   List of topics to preserve
    * @return
    *   List of topics failed to be removed from the consumer group
    */
  def ungroup(groupId: GroupId, keeps: List[TopicName] = Nil)(using F: Async[F]): F[List[TopicName]] = {
    val program: Resource[F, F[List[TopicName]]] = for {
      admin <- KafkaAdminClient.resource[F](settings.adminSettings)
      consumer <- makePureConsumer(PureConsumerSettings.withProperties(settings.consumerSettings.properties))
    } yield admin
      .listConsumerGroupOffsets(groupId.value)
      .partitionsToOffsetAndMetadata
      .map(_.keys.map(_.topic()).toList.distinct.diff(keeps))
      .flatMap(
        _.traverse { tn =>
          SnapshotConsumer[F](TopicName.applyUnsafe(tn), consumer).partitionsFor
            .flatMap(tps => admin.deleteConsumerGroupOffsets(groupId.value, tps.value.toSet))
            .attempt
            .map {
              case Left(_)  => Some(TopicName.applyUnsafe(tn))
              case Right(_) => None
            }
        }
      )
      .map(_.flatten)
    program.use(identity)
  }
}

object KafkaContext {

  /** Construct a KafkaContext from KafkaSettings. */
  def apply[F[_]](settings: KafkaSettings): KafkaContext[F] = new KafkaContext[F](settings)
}
