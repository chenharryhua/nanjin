package com.github.chenharryhua.nanjin.kafka

import cats.data.Reader
import cats.effect.kernel.{Async, Resource, Sync}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.UpdateConfig
import com.github.chenharryhua.nanjin.common.kafka.{TopicName, TopicNameC}
import com.github.chenharryhua.nanjin.kafka.streaming.{KafkaStreamsBuilder, NJStateStore}
import com.github.chenharryhua.nanjin.messages.kafka.codec.{NJAvroCodec, SerdeOf}
import fs2.kafka.*
import fs2.{Chunk, Pipe, Stream}
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.StreamsBuilder

import scala.util.Try

final class KafkaContext[F[_]](val settings: KafkaSettings)
    extends UpdateConfig[KafkaSettings, KafkaContext[F]] with Serializable {

  override def updateConfig(f: KafkaSettings => KafkaSettings): KafkaContext[F] =
    new KafkaContext[F](f(settings))

  def withGroupId(groupId: String): KafkaContext[F]     = updateConfig(_.withGroupId(groupId))
  def withApplicationId(appId: String): KafkaContext[F] = updateConfig(_.withApplicationId(appId))

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

  def consume(topicName: TopicName)(implicit F: Sync[F]): NJKafkaConsume[F] =
    new NJKafkaConsume[F](
      topicName,
      ConsumerSettings[F, Array[Byte], Array[Byte]](
        Deserializer[F, Array[Byte]],
        Deserializer[F, Array[Byte]]).withProperties(settings.consumerSettings.config),
      schemaRegistry.fetchAvroSchema(topicName),
      settings.schemaRegistrySettings
    )

  def consume(topicName: TopicNameC)(implicit F: Sync[F]): NJKafkaConsume[F] =
    consume(TopicName(topicName))

  def monitor(topicName: TopicName)(implicit F: Async[F]): Stream[F, String] =
    Stream.eval(schemaRegistry.fetchAvroSchema(topicName)).flatMap { schema =>
      val builder = new PullGenericRecord(settings.schemaRegistrySettings, topicName, schema)
      consume(topicName).stream.map(cr => builder.toJacksonString(cr.record))
    }

  def monitor(topicName: TopicNameC)(implicit F: Async[F]): Stream[F, String] =
    monitor(TopicName(topicName))

  def sink(topicName: TopicName)(implicit F: Async[F]): Pipe[F, Chunk[GenericRecord], Nothing] = {
    (ss: Stream[F, Chunk[GenericRecord]]) =>
      Stream.eval(schemaRegistry.fetchAvroSchema(topicName)).flatMap { schema =>
        val builder = new PushGenericRecord(settings.schemaRegistrySettings, topicName, schema)
        KafkaProducer[F]
          .stream(
            ProducerSettings[F, Array[Byte], Array[Byte]].withProperties(settings.producerSettings.config))
          .flatMap(kpd => ss.evalMap(ck => kpd.produce(ck.map(builder.fromGenericRecord)).flatten))
          .drain
      }
  }

  def store[K: SerdeOf, V: SerdeOf](storeName: TopicName): NJStateStore[K, V] =
    NJStateStore[K, V](
      storeName,
      settings.schemaRegistrySettings,
      RawKeyValueSerdePair[K, V](SerdeOf[K], SerdeOf[V]))
  def store[K: SerdeOf, V: SerdeOf](storeName: TopicNameC): NJStateStore[K, V] =
    store[K, V](TopicName(storeName))

  def buildStreams(topology: Reader[StreamsBuilder, Unit])(implicit F: Async[F]): KafkaStreamsBuilder[F] =
    streaming.KafkaStreamsBuilder[F](settings.streamSettings, topology)

  def buildStreams(topology: StreamsBuilder => Unit)(implicit F: Async[F]): KafkaStreamsBuilder[F] =
    buildStreams(Reader(topology))

  def shortLiveConsumer(topicName: TopicName)(implicit sync: Sync[F]): Resource[F, ShortLiveConsumer[F]] =
    ShortLiveConsumer(topicName, settings.consumerSettings.javaProperties)
}
