package com.github.chenharryhua.nanjin.kafka

import cats.effect.Resource
import cats.effect.kernel.{Async, Sync}
import cats.syntax.applicativeError.given
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import cats.syntax.traverse.given
import cats.{Endo, Parallel}
import com.github.chenharryhua.nanjin.common.UpdateConfig
import com.github.chenharryhua.nanjin.kafka.admins.{
  AdminTopic,
  AdminTopicGroup,
  SchemaRegistryApi,
  SnapshotConsumer
}
import com.github.chenharryhua.nanjin.kafka.connector.*
import com.github.chenharryhua.nanjin.kafka.serdes.{Primitive, Registered, Unregistered}
import com.github.chenharryhua.nanjin.kafka.streaming.{KafkaStreamsBuilder, StateStores, StreamsSerde}
import fs2.kafka.*
import io.confluent.kafka.schemaregistry.avro.{AvroSchema, AvroSchemaProvider}
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaRegistryClient}
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import org.apache.avro.Schema
import org.apache.kafka.streams.StreamsBuilder

import scala.jdk.CollectionConverters.given
import scala.util.Try

/** Context for Kafka operations including producers, consumers, schema registry, Kafka Streams, and
  * administrative tasks.
  *
  * @param settings
  *   KafkaSettings containing configuration for producers, consumers, schema registry, streams, and admin.
  * @tparam F
  *   Effect type
  */
final class KafkaContext[F[_]](val settings: KafkaSettings)
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
  def store[K, V](topic: TopicDef[K, V]): StateStores[K, V] =
    StateStores[K, V](topic.register(schema_registry_internal, settings.serdeSettings))

  /** Returns the registered Serde pair for a topic.
    *
    * @param topic
    *   Topic to register Serde for
    * @tparam K
    *   Key type
    * @tparam V
    *   Value type
    */
  def serde[K, V](topic: TopicDef[K, V]): TopicSerde[K, V] =
    topic.register(schema_registry_internal, settings.serdeSettings)

  def asKey[A](rs: Unregistered[A]): Registered[Key, A] =
    rs.asKey(schema_registry_internal, settings.serdeSettings.properties)

  def asValue[A](rs: Unregistered[A]): Registered[Value, A] =
    rs.asValue(schema_registry_internal, settings.serdeSettings.properties)

  // --------------------------------------------------------------------------
  // Schema Registry
  // --------------------------------------------------------------------------

  private lazy val schema_registry_internal: SchemaRegistryClient = {
    val url_config = AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG
    val baseUrl: String =
      settings.serdeSettings.properties.getOrElse(
        url_config,
        throw new IllegalStateException(s"Fatal error: $url_config is absent")
      ) // scalafix:ok

    val cacheCapacity: Int = settings.serdeSettings.properties
      .get(AbstractKafkaSchemaSerDeConfig.MAX_SCHEMAS_PER_SUBJECT_CONFIG)
      .flatMap(s => Try(s.toInt).toOption)
      .getOrElse(AbstractKafkaSchemaSerDeConfig.MAX_SCHEMAS_PER_SUBJECT_DEFAULT)

    new CachedSchemaRegistryClient(
      baseUrl,
      cacheCapacity,
      List(new AvroSchemaProvider, new JsonSchemaProvider, new ProtobufSchemaProvider).asJava,
      Map.empty.asJava)
  }

  /** Returns a SchemaRegistryApi for interacting with the configured Schema Registry.
    *
    * @throws java.lang.IllegalStateException
    *   if the URL config is absent
    */
  def schemaRegistry(using F: Sync[F]): SchemaRegistryApi[F] =
    SchemaRegistryApi[F](schema_registry_internal)

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
  def consume[K, V](topic: TopicDef[K, V])(using F: Async[F]): ConsumeKafka[F, K, V] =
    new ConsumeKafka[F, K, V](
      topic.topicName,
      topic.consumerSettings(schema_registry_internal, settings.serdeSettings, settings.consumerSettings)
    )

  def attemptConsume[K, V](topic: TopicDef[K, V])(using
    F: Async[F]): ConsumeKafka[F, Either[Throwable, K], Either[Throwable, V]] =
    new ConsumeKafka(
      topic.topicName,
      topic.attemptConsumerSettings(
        schema_registry_internal,
        settings.serdeSettings,
        settings.consumerSettings)
    )

  def consume[K, V](
    topicName: TopicName,
    k: Resource[F, KeyDeserializer[F, K]],
    v: Resource[F, ValueDeserializer[F, V]])(using F: Async[F]): ConsumeKafka[F, K, V] =
    new ConsumeKafka[F, K, V](
      topicName,
      ConsumerSettings(using k, v).withProperties(settings.consumerSettings.properties)
    )

  /** Create a raw byte consumer
    */
  def consumeBytes(topicName: TopicName)(using F: Async[F]): ConsumeKafka[F, Array[Byte], Array[Byte]] = {
    val topicDef = TopicDef(topicName, Primitive[Array[Byte]], Primitive[Array[Byte]])
    consume(topicDef)
  }

  def consumeGenericRecord(topicName: TopicName, key: Option[Schema] = None, value: Option[Schema] = None)(
    using F: Async[F]): ConsumeGenericRecord[F] =
    ConsumeGenericRecord[F](
      topicName = topicName,
      schemaPair = OptionalAvroSchemaPair(key.map(AvroSchema(_)), value.map(AvroSchema(_))),
      fromSchemaRegistry = schemaRegistry.fetchOptionalAvroSchema(topicName),
      ConsumerSettings[F, Array[Byte], Array[Byte]](
        Deserializer[F, Array[Byte]],
        Deserializer[F, Array[Byte]])
        .withProperties(settings.consumerSettings.properties)
    )

  // --------------------------------------------------------------------------
  // Producers
  // --------------------------------------------------------------------------

  def produce[K, V](topic: TopicDef[K, V])(using F: Async[F], ev: Parallel[F]): ProduceKafka[F, K, V] =
    new ProduceKafka[F, K, V](
      topic.topicName,
      topic.producerSettings[F](schema_registry_internal, settings.serdeSettings, settings.producerSettings))

  def produce[K, V](
    topicName: TopicName,
    k: Resource[F, KeySerializer[F, K]],
    v: Resource[F, ValueSerializer[F, V]])(using F: Async[F], ev: Parallel[F]): ProduceKafka[F, K, V] =
    new ProduceKafka[F, K, V](
      topicName,
      ProducerSettings[F, K, V](using k, v).withProperties(settings.producerSettings.properties))

  def produceGenericRecord(topicName: TopicName, key: Option[Schema] = None, value: Option[Schema] = None)(
    using
    F: Async[F],
    ev: Parallel[F]): ProduceGenericRecord[F] =
    ProduceGenericRecord[F](
      topicName = topicName,
      schemaPair = OptionalAvroSchemaPair(key.map(AvroSchema(_)), value.map(AvroSchema(_))),
      srClient = schema_registry_internal,
      producerSettings =
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
      schema_registry_internal,
      settings.serdeSettings,
      topology)

  // --------------------------------------------------------------------------
  // Admin
  // --------------------------------------------------------------------------

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
    *   List of topics successfully be removed from the consumer group
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
          SnapshotConsumer[F](TopicName(tn), consumer).partitionsFor
            .flatMap(tps => admin.deleteConsumerGroupOffsets(groupId.value, tps.toSet))
            .attempt
            .map {
              case Left(_)  => None
              case Right(_) => Some(TopicName(tn))
            }
        }
      )
      .map(_.flatten)
    program.use(identity)
  }
}
