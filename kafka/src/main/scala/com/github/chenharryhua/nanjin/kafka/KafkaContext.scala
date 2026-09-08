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
import com.github.chenharryhua.nanjin.kafka.config.KafkaSettings
import com.github.chenharryhua.nanjin.kafka.connector.*
import com.github.chenharryhua.nanjin.kafka.serdes.{Registered, Unregistered}
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

/** Entry point for Kafka operations: producers, consumers, schema registry, Kafka Streams, and administrative
  * tasks, all derived from a single KafkaSettings.
  *
  * A context is cheap to hold and immutable; updateConfig returns a new context with adjusted settings. The
  * factory methods build effectful producers/consumers/admin resources that run later. The schema registry
  * client is created lazily on first use and shared across the operations that need it. Obtain one via
  * KafkaContext[F](settings) or settings.context[F].
  *
  * @tparam F
  *   effect type
  */
sealed trait KafkaContext[F[_]] extends UpdateConfig[KafkaSettings, KafkaContext[F]] {

  /** The Kafka settings backing this context. */
  def settings: KafkaSettings

  /** Returns a new KafkaContext with updated settings. */
  override def updateConfig(f: Endo[KafkaSettings]): KafkaContext[F]

  /** Returns a SchemaRegistryApi for interacting with the configured Schema Registry.
    *
    * @throws java.lang.IllegalStateException
    *   if the URL config is absent
    */
  def schemaRegistry(using F: Sync[F]): SchemaRegistryApi[F]

  /** Register the key/value Serdes for a topic against the schema registry.
    *
    * @param topic
    *   the topic definition carrying key/value schema information
    */
  def serde[K, V](topic: TopicDef[K, V]): TopicSerde[K, V]

  /** Create Kafka Streams state stores for a topic, using its registered Serdes.
    *
    * @param topic
    *   the topic definition carrying key/value schema information
    */
  def store[K, V](topic: TopicDef[K, V]): StateStores[K, V]

  /** Register an unregistered Serde as a topic key Serde against the schema registry. */
  def asKey[A](rs: Unregistered[A]): Registered[Key, A]

  /** Register an unregistered Serde as a topic value Serde against the schema registry. */
  def asValue[A](rs: Unregistered[A]): Registered[Value, A]

  /** Create a typed consumer for a topic, deserializing keys and values via its registered Serdes.
    *
    * @param topic
    *   the topic definition to consume
    */
  def consume[K, V](topic: TopicDef[K, V])(using F: Async[F]): ConsumeKafka[F, K, V]

  /** Like consume, but each key and value is decoded independently and deserialization failures surface as
    * Left rather than aborting the stream. Useful for tolerating poison records.
    *
    * @param topic
    *   the topic definition to consume
    */
  def attemptConsume[K, V](topic: TopicDef[K, V])(using
    F: Async[F]): ConsumeKafka[F, Either[Throwable, K], Either[Throwable, V]]

  /** Create a typed consumer from explicit key/value deserializers, bypassing the schema registry.
    *
    * @param topicName
    *   the topic to consume
    * @param k
    *   key deserializer resource
    * @param v
    *   value deserializer resource
    */
  def consume[K, V](
    topicName: String,
    k: Resource[F, KeyDeserializer[F, K]],
    v: Resource[F, ValueDeserializer[F, V]])(using F: Async[F]): ConsumeKafka[F, K, V]

  /** Create a raw consumer that yields the topic bytes without deserialization.
    *
    * @param topicName
    *   the topic to consume
    */
  def consumeBytes(topicName: String)(using F: Async[F]): ConsumeKafka[F, Array[Byte], Array[Byte]]

  /** Consume Avro GenericRecords, resolving schemas from the registry and/or the optional overrides.
    *
    * @param topicName
    *   the topic to consume
    * @param key
    *   optional key reader schema; when absent the registry schema is used
    * @param value
    *   optional value reader schema; when absent the registry schema is used
    */
  def consumeGenericRecord(topicName: String, key: Option[Schema] = None, value: Option[Schema] = None)(using
    F: Async[F]): ConsumeGenericRecord[F]

  /** Create a typed producer for a topic, serializing keys and values via its registered Serdes.
    *
    * @param topic
    *   the topic definition to produce to
    */
  def produce[K, V](topic: TopicDef[K, V])(using F: Async[F], ev: Parallel[F]): ProduceKafka[F, K, V]

  /** Create a typed producer from explicit key/value serializers, bypassing the schema registry.
    *
    * @param topicName
    *   the topic to produce to
    * @param k
    *   key serializer resource
    * @param v
    *   value serializer resource
    */
  def produce[K, V](
    topicName: String,
    k: Resource[F, KeySerializer[F, K]],
    v: Resource[F, ValueSerializer[F, V]])(using F: Async[F], ev: Parallel[F]): ProduceKafka[F, K, V]

  /** Produce Avro GenericRecords, registering/resolving schemas via the registry and optional overrides.
    *
    * @param topicName
    *   the topic to produce to
    * @param key
    *   optional key schema override
    * @param value
    *   optional value schema override
    */
  def produceGenericRecord(topicName: String, key: Option[Schema] = None, value: Option[Schema] = None)(using
    F: Async[F],
    ev: Parallel[F]): ProduceGenericRecord[F]

  /** Build a Kafka Streams topology under the given application id.
    *
    * @param applicationId
    *   Kafka Streams application id
    * @param topology
    *   builds the topology; receives a StreamsBuilder and a StreamsSerde for registry-backed Serdes
    */
  def buildStreams(applicationId: String)(topology: (StreamsBuilder, StreamsSerde) => Unit)(using
    F: Async[F]): KafkaStreamsBuilder[F]

  /** A raw Kafka AdminClient resource for cluster/topic administration. */
  def admin(using F: Async[F]): Resource[F, KafkaAdminClient[F]]

  /** An admin view scoped to a single topic and consumer group, backed by an admin client and a snapshot
    * consumer.
    *
    * @param topicName
    *   the topic to administer
    * @param groupId
    *   the consumer group to inspect
    */
  def admin(topicName: String, groupId: String)(using F: Async[F]): Resource[F, AdminTopicGroup[F]]

  /** An admin view scoped to a single topic, backed by an admin client and a snapshot consumer.
    *
    * @param topicName
    *   the topic to administer
    */
  def admin(topicName: String)(using F: Async[F]): Resource[F, AdminTopic[F]]

  /** Remove a consumer groups committed offsets for every topic except those in keeps.
    *
    * @param groupId
    *   the consumer group to prune
    * @param keeps
    *   topics whose offsets are preserved
    * @return
    *   the topics whose offsets were removed from the group
    */
  def ungroup(groupId: String, keeps: List[String] = Nil)(using F: Async[F]): F[List[TopicName]]
}

object KafkaContext {
  def apply[F[_]](settings: KafkaSettings): KafkaContext[F] =
    new Impl[F](settings)

  final private class Impl[F[_]] private[KafkaContext] (val settings: KafkaSettings) extends KafkaContext[F] {

    private lazy val schema_registry_internal: SchemaRegistryClient = {
      val url_config = AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG
      val baseUrl: String =
        settings.serdeSettings.properties.getOrElse(
          url_config,
          throw SchemaRegistryUrlAbsent(url_config)
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

    override def schemaRegistry(using F: Sync[F]): SchemaRegistryApi[F] =
      SchemaRegistryApi[F](schema_registry_internal)

    override def updateConfig(f: Endo[KafkaSettings]): KafkaContext[F] =
      new Impl[F](f(settings))

    override def serde[K, V](topic: TopicDef[K, V]): TopicSerde[K, V] =
      topic.register(schema_registry_internal, settings.serdeSettings)

    override def store[K, V](topic: TopicDef[K, V]): StateStores[K, V] =
      StateStores[K, V](serde(topic))

    override def asKey[A](rs: Unregistered[A]): Registered[Key, A] =
      rs.asKey(schema_registry_internal, settings.serdeSettings.properties)

    override def asValue[A](rs: Unregistered[A]): Registered[Value, A] =
      rs.asValue(schema_registry_internal, settings.serdeSettings.properties)

    override def consume[K, V](topic: TopicDef[K, V])(using F: Async[F]): ConsumeKafka[F, K, V] =
      new ConsumeKafka[F, K, V](
        topic.topicName,
        topic.consumerSettings(schema_registry_internal, settings.serdeSettings, settings.consumerSettings)
      )

    override def attemptConsume[K, V](topic: TopicDef[K, V])(using
      F: Async[F]): ConsumeKafka[F, Either[Throwable, K], Either[Throwable, V]] =
      new ConsumeKafka(
        topic.topicName,
        topic.attemptConsumerSettings(
          schema_registry_internal,
          settings.serdeSettings,
          settings.consumerSettings)
      )

    override def consume[K, V](
      topicName: String,
      k: Resource[F, KeyDeserializer[F, K]],
      v: Resource[F, ValueDeserializer[F, V]])(using F: Async[F]): ConsumeKafka[F, K, V] =
      new ConsumeKafka[F, K, V](
        TopicName(topicName),
        ConsumerSettings(using k, v).withProperties(settings.consumerSettings.properties)
      )

    override def consumeBytes(topicName: String)(using
      F: Async[F]): ConsumeKafka[F, Array[Byte], Array[Byte]] =
      consume(
        topicName,
        Resource.pure[F, KeyDeserializer[F, Array[Byte]]](Deserializer[F, Array[Byte]]),
        Resource.pure[F, ValueDeserializer[F, Array[Byte]]](Deserializer[F, Array[Byte]])
      )

    override def consumeGenericRecord(
      topicName: String,
      key: Option[Schema] = None,
      value: Option[Schema] = None)(using F: Async[F]): ConsumeGenericRecord[F] = {
      val tn: TopicName = TopicName(topicName)
      ConsumeGenericRecord[F](
        topicName = tn,
        schemaPair = OptionalAvroSchemaPair(key.map(AvroSchema(_)), value.map(AvroSchema(_))),
        fromSchemaRegistry = schemaRegistry.fetchOptionalAvroSchema(tn),
        ConsumerSettings[F, Array[Byte], Array[Byte]](
          Deserializer[F, Array[Byte]],
          Deserializer[F, Array[Byte]])
          .withProperties(settings.consumerSettings.properties)
      )
    }

    override def produce[K, V](
      topic: TopicDef[K, V])(using F: Async[F], ev: Parallel[F]): ProduceKafka[F, K, V] =
      new ProduceKafka[F, K, V](
        topic.topicName,
        topic.producerSettings[F](
          schema_registry_internal,
          settings.serdeSettings,
          settings.producerSettings))

    override def produce[K, V](
      topicName: String,
      k: Resource[F, KeySerializer[F, K]],
      v: Resource[F, ValueSerializer[F, V]])(using F: Async[F], ev: Parallel[F]): ProduceKafka[F, K, V] =
      new ProduceKafka[F, K, V](
        TopicName(topicName),
        ProducerSettings[F, K, V](using k, v).withProperties(settings.producerSettings.properties))

    override def produceGenericRecord(
      topicName: String,
      key: Option[Schema] = None,
      value: Option[Schema] = None)(using F: Async[F], ev: Parallel[F]): ProduceGenericRecord[F] =
      ProduceGenericRecord[F](
        topicName = TopicName(topicName),
        schemaPair = OptionalAvroSchemaPair(key.map(AvroSchema(_)), value.map(AvroSchema(_))),
        srClient = schema_registry_internal,
        serdeSettings = settings.serdeSettings,
        producerSettings = ProducerSettings[F, Array[Byte], Array[Byte]](
          Serializer[F, Array[Byte]],
          Serializer[F, Array[Byte]])
          .withProperties(settings.producerSettings.properties)
      )

    override def buildStreams(applicationId: String)(topology: (StreamsBuilder, StreamsSerde) => Unit)(using
      F: Async[F]): KafkaStreamsBuilder[F] =
      streaming.KafkaStreamsBuilder[F](
        applicationId,
        settings.streamSettings,
        schema_registry_internal,
        settings.serdeSettings,
        topology)

    override def admin(using F: Async[F]): Resource[F, KafkaAdminClient[F]] =
      KafkaAdminClient.resource[F](settings.adminSettings)

    private def snapshotConsumer(topicName: TopicName, groupId: Option[GroupId])(using
      F: Async[F]): Resource[F, SnapshotConsumer[F]] = {
      val baseSettings = PureConsumerSettings
        .withProperties(settings.consumerSettings.properties)
        .withAutoOffsetReset(AutoOffsetReset.None)
        .withEnableAutoCommit(false)

      val consumerSettings = groupId.fold(baseSettings)(gid => baseSettings.withGroupId(gid.value))
      SnapshotConsumer(topicName, consumerSettings)
    }

    override def admin(topicName: String, groupId: String)(using
      F: Async[F]): Resource[F, AdminTopicGroup[F]] = {
      val tn: TopicName = TopicName(topicName)
      val gid: GroupId = GroupId(groupId)
      for {
        admin <- KafkaAdminClient.resource[F](settings.adminSettings)
        consumer <- snapshotConsumer(tn, Some(gid))
      } yield AdminTopicGroup(admin, consumer, tn, gid)
    }

    override def admin(topicName: String)(using F: Async[F]): Resource[F, AdminTopic[F]] = {
      val tn: TopicName = TopicName(topicName)
      for {
        admin <- KafkaAdminClient.resource[F](settings.adminSettings)
        consumer <- snapshotConsumer(tn, None)
      } yield AdminTopic(admin, consumer, tn)
    }

    override def ungroup(
      groupId: String,
      keeps: List[String] = Nil)(using F: Async[F]): F[List[TopicName]] = {
      val program: Resource[F, F[List[TopicName]]] = for {
        admin <- KafkaAdminClient.resource[F](settings.adminSettings)
        consumer <- makePureConsumer(
          PureConsumerSettings.withProperties(settings.consumerSettings.properties))
      } yield admin
        .listConsumerGroupOffsets(groupId)
        .partitionsToOffsetAndMetadata
        .map(_.keys.map(_.topic()).toList.distinct.diff(keeps))
        .flatMap(
          _.traverse { tn =>
            SnapshotConsumer[F](TopicName(tn), consumer).partitionsFor
              .flatMap(tps => admin.deleteConsumerGroupOffsets(groupId, tps.toSet))
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
}
