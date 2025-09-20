package com.github.chenharryhua.nanjin.kafka

import cats.Endo
import cats.effect.Resource
import cats.effect.kernel.{Async, Sync}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.kafka.{TopicName, TopicNameL}
import com.github.chenharryhua.nanjin.common.{utils, UpdateConfig}
import com.github.chenharryhua.nanjin.kafka.connector.*
import com.github.chenharryhua.nanjin.kafka.streaming.{KafkaStreamsBuilder, StateStores, StreamsSerde}
import com.github.chenharryhua.nanjin.messages.kafka.CRMetaInfo
import com.github.chenharryhua.nanjin.messages.kafka.codec.*
import fs2.Stream
import fs2.kafka.*
import io.circe.syntax.EncoderOps
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import org.apache.kafka.streams.scala.StreamsBuilder

import java.time.Instant
import scala.util.Try

final class KafkaContext[F[_]] private (val settings: KafkaSettings)
    extends UpdateConfig[KafkaSettings, KafkaContext[F]] with Serializable {

  override def updateConfig(f: Endo[KafkaSettings]): KafkaContext[F] =
    new KafkaContext[F](f(settings))

  def store[K, V](topic: KafkaTopic[K, V]): StateStores[K, V] =
    StateStores[K, V](topic.register(settings.schemaRegistrySettings))

  def serde[K, V](topic: KafkaTopic[K, V]): KafkaGenericSerde[K, V] =
    topic.register(settings.schemaRegistrySettings)

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

  /*
   * consumer
   */

  def consume[K, V](topic: KafkaTopic[K, V])(implicit F: Sync[F]): ConsumeKafka[F, K, V] =
    new ConsumeKafka[F, K, V](
      topic.topicName,
      topic.consumerSettings(settings.schemaRegistrySettings, settings.consumerSettings)
    )

  def consumeAvro(topicName: TopicNameL)(implicit F: Sync[F]): ConsumeGenericRecord[F] =
    new ConsumeGenericRecord[F](
      TopicName(topicName),
      schemaRegistry.fetchOptionalAvroSchema(TopicName(topicName)),
      identity,
      ConsumerSettings[F, Array[Byte], Array[Byte]](
        Deserializer[F, Array[Byte]],
        Deserializer[F, Array[Byte]]).withProperties(settings.consumerSettings.properties)
    )

  /** Monitor topic from a given instant
    */
  def monitor(topicName: TopicNameL, from: Instant = Instant.now())(implicit F: Async[F]): Stream[F, String] =
    Stream.eval(utils.randomUUID[F]).flatMap { uuid =>
      consumeAvro(topicName)
        .updateConfig( // avoid accidentally join an existing consumer-group
          _.withGroupId(uuid.show).withEnableAutoCommit(false))
        .assign(from)
        .map { ccr =>
          val rcd = ccr.record
          rcd.value
            .flatMap(gr2Jackson)
            .toEither
            .leftMap(e => new Exception(CRMetaInfo(ccr.record).asJson.noSpaces, e))
        }
        .rethrow
    }

  /*
   * producer
   */

  def produce[K, V](topic: KafkaTopic[K, V])(implicit F: Sync[F]): ProduceKeyValuePair[F, K, V] =
    new ProduceKeyValuePair[F, K, V](
      topic.topicName,
      topic.producerSettings(settings.schemaRegistrySettings, settings.producerSettings)
    )

  def sharedProduce[K, V](pair: SerdePair[K, V])(implicit F: Sync[F]): ProduceKafka[F, K, V] =
    new ProduceKafka[F, K, V](
      pair.producerSettings(settings.schemaRegistrySettings, settings.producerSettings)
    )

  def produceAvro(topicName: TopicNameL)(implicit F: Sync[F]): ProduceGenericRecord[F] =
    new ProduceGenericRecord[F](
      TopicName(topicName),
      schemaRegistry.fetchOptionalAvroSchema(TopicName(topicName)),
      identity,
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
