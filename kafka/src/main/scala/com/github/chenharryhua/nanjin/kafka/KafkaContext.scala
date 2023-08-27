package com.github.chenharryhua.nanjin.kafka

import cats.Endo
import cats.data.Reader
import cats.effect.kernel.{Async, Sync}
import cats.effect.std.UUIDGen
import cats.implicits.toShow
import com.github.chenharryhua.nanjin.common.UpdateConfig
import com.github.chenharryhua.nanjin.common.kafka.{TopicName, TopicNameC}
import com.github.chenharryhua.nanjin.kafka.streaming.{KafkaStreamsBuilder, NJStateStore}
import com.github.chenharryhua.nanjin.messages.kafka.codec.{NJAvroCodec, SerdeOf}
import fs2.Stream
import fs2.kafka.*
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.StreamsBuilder

import scala.util.Try

final class KafkaContext[F[_]](val settings: KafkaSettings)
    extends UpdateConfig[KafkaSettings, KafkaContext[F]] with Serializable {

  override def updateConfig(f: Endo[KafkaSettings]): KafkaContext[F] =
    new KafkaContext[F](f(settings))

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

  def topic[K: SerdeOf, V: SerdeOf](topicName: TopicNameC): KafkaTopic[F, K, V] =
    topic[K, V](TopicName(topicName))

  @transient lazy val schemaRegistry: SchemaRegistryApi[F] = {
    val url_config = AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG
    val url = settings.schemaRegistrySettings.config.get(url_config) match {
      case Some(value) => value
      case None        => throw new Exception(s"$url_config is absent")
    }
    val cacheCapacity: Int = settings.schemaRegistrySettings.config
      .get(AbstractKafkaSchemaSerDeConfig.MAX_SCHEMAS_PER_SUBJECT_CONFIG)
      .flatMap(s => Try(s.toInt).toOption)
      .getOrElse(AbstractKafkaSchemaSerDeConfig.MAX_SCHEMAS_PER_SUBJECT_DEFAULT)
    new SchemaRegistryApi[F](new CachedSchemaRegistryClient(url, cacheCapacity))
  }

  def consume(topicName: TopicName)(implicit F: Sync[F]): NJKafkaByteConsume[F] =
    new NJKafkaByteConsume[F](
      topicName,
      ConsumerSettings[F, Array[Byte], Array[Byte]](
        Deserializer[F, Array[Byte]],
        Deserializer[F, Array[Byte]]).withProperties(settings.consumerSettings.properties),
      schemaRegistry.fetchAvroSchema(topicName),
      settings.schemaRegistrySettings
    )

  def consume(topicName: TopicNameC)(implicit F: Sync[F]): NJKafkaByteConsume[F] =
    consume(TopicName(topicName))

  def monitor(topicName: TopicName)(implicit F: Async[F], U: UUIDGen[F]): Stream[F, String] =
    Stream.eval(U.randomUUID).flatMap { uuid =>
      consume(topicName)
        .updateConfig( // avoid accidentally join an existing consumer-group
          _.withGroupId(uuid.show).withEnableAutoCommit(false).withAutoOffsetReset(AutoOffsetReset.Latest))
        .jackson
        .map(_.record.value)
    }

  def monitor(topicName: TopicNameC)(implicit F: Async[F]): Stream[F, String] =
    monitor(TopicName(topicName))

  def sink(topicName: TopicName)(implicit F: Sync[F]): NJGenericRecordSink[F] =
    new NJGenericRecordSink[F](
      topicName,
      ProducerSettings[F, Array[Byte], Array[Byte]](Serializer[F, Array[Byte]], Serializer[F, Array[Byte]])
        .withProperties(settings.producerSettings.properties),
      schemaRegistry.fetchAvroSchema(topicName),
      settings.schemaRegistrySettings
    )

  def sink(topicName: TopicNameC)(implicit F: Sync[F]): NJGenericRecordSink[F] =
    sink(TopicName(topicName))

  def store[K: SerdeOf, V: SerdeOf](storeName: TopicName): NJStateStore[K, V] =
    NJStateStore[K, V](
      storeName,
      settings.schemaRegistrySettings,
      RawKeyValueSerdePair[K, V](SerdeOf[K], SerdeOf[V]))

  def store[K: SerdeOf, V: SerdeOf](storeName: TopicNameC): NJStateStore[K, V] =
    store(TopicName(storeName))

  def buildStreams(applicationId: String, topology: Reader[StreamsBuilder, Unit])(implicit
    F: Async[F]): KafkaStreamsBuilder[F] =
    streaming.KafkaStreamsBuilder[F](applicationId, settings.streamSettings, topology)

  def buildStreams(applicationId: String, topology: StreamsBuilder => Unit)(implicit
    F: Async[F]): KafkaStreamsBuilder[F] =
    buildStreams(applicationId, Reader(topology))

  def admin(topicName: TopicName)(implicit F: Async[F]): KafkaAdminApi[F] =
    KafkaAdminApi[F](topicName, settings.consumerSettings, settings.adminSettings)

  def admin(topicName: TopicNameC)(implicit F: Async[F]): KafkaAdminApi[F] =
    admin(TopicName(topicName))
}
