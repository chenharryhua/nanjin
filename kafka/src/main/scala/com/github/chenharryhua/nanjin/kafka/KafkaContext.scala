package com.github.chenharryhua.nanjin.kafka

import cats.Endo
import cats.effect.Resource
import cats.effect.kernel.{Async, Sync}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.kafka.{TopicName, TopicNameL}
import com.github.chenharryhua.nanjin.common.{utils, UpdateConfig}
import com.github.chenharryhua.nanjin.kafka.connector.{
  ConsumeByteKafka,
  ConsumeKafka,
  ProduceByteKafka,
  ProduceKafka,
  PushGenericRecord
}
import com.github.chenharryhua.nanjin.kafka.streaming.{KafkaStreamsBuilder, StateStores, StreamsSerde}
import com.github.chenharryhua.nanjin.messages.kafka.CRMetaInfo
import com.github.chenharryhua.nanjin.messages.kafka.codec.*
import fs2.kafka.*
import fs2.Stream
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

  def store[K, V](topicDef: TopicDef[K, V]): StateStores[K, V] = {
    val pair = topicDef.codecPair.register(settings.schemaRegistrySettings, topicDef.topicName)
    StateStores[K, V](pair)
  }

  def serde[K, V](topicDef: TopicDef[K, V]): KafkaGenericSerde[K, V] = {
    val pair = topicDef.codecPair.register(settings.schemaRegistrySettings, topicDef.topicName)
    new KafkaGenericSerde[K, V](pair.key, pair.value)
  }

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

  def consume[K, V](topicDef: TopicDef[K, V])(implicit F: Sync[F]): ConsumeKafka[F, K, V] = {
    val topic: KafkaTopic[K, V] =
      topicDef.codecPair.register(settings.schemaRegistrySettings, topicDef.topicName)
    new ConsumeKafka[F, K, V](
      topicDef.topicName,
      ConsumerSettings[F, K, V](
        Deserializer.delegate[F, K](topic.key.registered.serde.deserializer()),
        Deserializer.delegate[F, V](topic.value.registered.serde.deserializer())
      ).withProperties(settings.consumerSettings.properties)
    )
  }

  def consume(topicName: TopicNameL)(implicit F: Sync[F]): ConsumeByteKafka[F] =
    new ConsumeByteKafka[F](
      TopicName(topicName),
      schemaRegistry.fetchAvroSchema(topicName),
      ConsumerSettings[F, Array[Byte], Array[Byte]](
        Deserializer[F, Array[Byte]],
        Deserializer[F, Array[Byte]]).withProperties(settings.consumerSettings.properties)
    )

  def consume(topicName: TopicNameL, avroSchemaPair: AvroSchemaPair)(implicit
    F: Sync[F]): ConsumeByteKafka[F] =
    new ConsumeByteKafka[F](
      TopicName(topicName),
      F.pure(avroSchemaPair),
      ConsumerSettings[F, Array[Byte], Array[Byte]](
        Deserializer[F, Array[Byte]],
        Deserializer[F, Array[Byte]]).withProperties(settings.consumerSettings.properties)
    )

  /** Monitor topic from a given instant
    */
  def monitor(topicName: TopicNameL, from: Instant = Instant.now())(implicit F: Async[F]): Stream[F, String] =
    Stream.eval(utils.randomUUID[F]).flatMap { uuid =>
      consume(topicName)
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

  def produce[K: AvroCodecOf, V: AvroCodecOf](implicit F: Sync[F]): ProduceKafka[F, K, V] =
    new ProduceKafka[F, K, V](
      ProducerSettings[F, K, V](
        Serializer.delegate(AvroCodecOf[K].asKey(settings.schemaRegistrySettings.config).serde.serializer()),
        Serializer.delegate(AvroCodecOf[V].asValue(settings.schemaRegistrySettings.config).serde.serializer())
      ).withProperties(settings.producerSettings.properties)
    )

  def produce[K, V](raw: AvroCodecPair[K, V])(implicit F: Sync[F]): ProduceKafka[F, K, V] =
    produce[K, V](raw.key, raw.value, Sync[F])

  def produce(topicName: TopicNameL, avroSchemaPair: AvroSchemaPair)(implicit F: Sync[F]) =
    new ProduceByteKafka[F](
      F.pure(new PushGenericRecord(settings.schemaRegistrySettings, TopicName(topicName), avroSchemaPair)),
      ProducerSettings[F, Array[Byte], Array[Byte]](Serializer[F, Array[Byte]], Serializer[F, Array[Byte]])
        .withProperties(settings.producerSettings.properties)
    )

  def produce(topicName: TopicNameL)(implicit F: Sync[F]): ProduceByteKafka[F] = {
    val push = schemaRegistry
      .fetchAvroSchema(topicName)
      .map(pair => new PushGenericRecord(settings.schemaRegistrySettings, TopicName(topicName), pair))

    new ProduceByteKafka[F](
      push,
      ProducerSettings[F, Array[Byte], Array[Byte]](Serializer[F, Array[Byte]], Serializer[F, Array[Byte]])
        .withProperties(settings.producerSettings.properties)
    )
  }

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
