package com.github.chenharryhua.nanjin.kafka

import cats.Endo
import cats.data.Reader
import cats.effect.Resource
import cats.effect.kernel.{Async, Sync}
import cats.effect.std.UUIDGen
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.UpdateConfig
import com.github.chenharryhua.nanjin.common.kafka.{TopicName, TopicNameL}
import com.github.chenharryhua.nanjin.kafka.streaming.{KafkaStreamsBuilder, NJStateStore}
import com.github.chenharryhua.nanjin.messages.kafka.codec.*
import fs2.Stream
import fs2.kafka.*
import io.circe.Json
import io.circe.jawn.parse
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.StreamsBuilder

import scala.util.Try

final class KafkaContext[F[_]] private (val settings: KafkaSettings)
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
    new KafkaTopic[F, K, V](topicDef, settings)

  def topic[K: SerdeOf, V: SerdeOf](topicName: TopicName): KafkaTopic[F, K, V] =
    topic[K, V](TopicDef[K, V](topicName))

  def topic[K: SerdeOf, V: SerdeOf](topicName: TopicNameL): KafkaTopic[F, K, V] =
    topic[K, V](TopicName(topicName))

  def jsonTopic(topicName: TopicName): KafkaTopic[F, KJson[Json], KJson[Json]] =
    topic[KJson[Json], KJson[Json]](topicName)

  def jsonTopic(topicName: TopicNameL): KafkaTopic[F, KJson[Json], KJson[Json]] =
    jsonTopic(TopicName(topicName))

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

  def consume(topicName: TopicNameL)(implicit F: Sync[F]): NJKafkaByteConsume[F] =
    consume(TopicName(topicName))

  def monitor(topicName: TopicNameL, f: AutoOffsetReset.type => AutoOffsetReset = _.Latest)(implicit
    F: Async[F]): Stream[F, String] =
    Stream.eval(UUIDGen[F].randomUUID).flatMap { uuid =>
      consume(TopicName(topicName))
        .updateConfig( // avoid accidentally join an existing consumer-group
          _.withGroupId(uuid.show).withEnableAutoCommit(false).withAutoOffsetReset(f(AutoOffsetReset)))
        .genericRecords
        .map { ccr =>
          val rcd = ccr.record
          rcd.value
            .flatMap(gr2Jackson(_))
            .toEither
            .leftMap(e =>
              new Exception(s"topic=${rcd.topic}, partition=${rcd.partition}, offset=${rcd.offset}", e))
        }
        .rethrow
    }

  private def bytesProducerSettings(implicit F: Sync[F]): ProducerSettings[F, Array[Byte], Array[Byte]] =
    ProducerSettings[F, Array[Byte], Array[Byte]](Serializer[F, Array[Byte]], Serializer[F, Array[Byte]])
      .withProperties(settings.producerSettings.properties)

  def sink(topicName: TopicName)(implicit F: Sync[F]): NJGenericRecordSinkBuilder[F] =
    new NJGenericRecordSinkBuilder[F](
      topicName,
      bytesProducerSettings,
      schemaRegistry.fetchAvroSchema(topicName),
      settings.schemaRegistrySettings
    )

  def sink(topicName: TopicNameL)(implicit F: Sync[F]): NJGenericRecordSinkBuilder[F] =
    sink(TopicName(topicName))

  def produce(jackson: String)(implicit F: Async[F]): F[ProducerResult[Array[Byte], Array[Byte]]] =
    for {
      tn <- F.fromEither(parse(jackson).flatMap(_.hcursor.get[String]("topic")))
      topicName <- F.fromEither(TopicName.from(tn))
      schemaPair <- schemaRegistry.fetchAvroSchema(topicName)
      gr <- F.fromTry(jackson2GR(schemaPair.consumerSchema, jackson))
      builder = new PushGenericRecord(settings.schemaRegistrySettings, topicName, schemaPair)
      res <- KafkaProducer
        .resource(bytesProducerSettings)
        .use(_.produce(ProducerRecords.one(builder.fromGenericRecord(gr))).flatten)
    } yield res

  /** upload records which are downloaded from Confluent Kafka Control Center
    */
  def confluentProduce(confluent: String)(implicit
    F: Async[F]): F[List[ProducerResult[Array[Byte], Array[Byte]]]] = {
    def sendOne(json: Json): F[ProducerResult[Array[Byte], Array[Byte]]] = for {
      topicName <- F.fromEither(json.hcursor.get[String]("topic").flatMap(TopicName.from))
      schemaPair <- schemaRegistry.fetchAvroSchema(topicName)
      key <- F.fromTry(
        json.hcursor.get[Json]("key").toTry.flatMap(js => jackson2GR(schemaPair.key, js.noSpaces)))
      value <- F.fromTry(
        json.hcursor.get[Json]("value").toTry.flatMap(js => jackson2GR(schemaPair.value, js.noSpaces)))
      builder = new PushGenericRecord(settings.schemaRegistrySettings, topicName, schemaPair)
      pr <- KafkaProducer
        .resource(bytesProducerSettings)
        .use(_.produce(ProducerRecords.one(builder.fromGenericRecord(key, value))).flatten)
    } yield pr

    for {
      jsons <- F.fromEither(parse(confluent).flatMap(_.as[List[Json]]))
      prs <- jsons.traverse(sendOne)
    } yield prs
  }

  def store[K: SerdeOf, V: SerdeOf](storeName: TopicName): NJStateStore[K, V] =
    NJStateStore[K, V](
      storeName,
      settings.schemaRegistrySettings,
      RawKeyValueSerdePair[K, V](SerdeOf[K], SerdeOf[V]))

  def store[K: SerdeOf, V: SerdeOf](storeName: TopicNameL): NJStateStore[K, V] =
    store(TopicName(storeName))

  def buildStreams(applicationId: String, topology: Reader[StreamsBuilder, Unit])(implicit
    F: Async[F]): KafkaStreamsBuilder[F] =
    streaming.KafkaStreamsBuilder[F](applicationId, settings.streamSettings, topology)

  def buildStreams(applicationId: String, topology: StreamsBuilder => Unit)(implicit
    F: Async[F]): KafkaStreamsBuilder[F] =
    buildStreams(applicationId, Reader(topology))

  def adminR(implicit F: Async[F]): Resource[F, KafkaAdminClient[F]] =
    KafkaAdminClient.resource[F](settings.adminSettings)

  def admin(topicName: TopicName)(implicit F: Async[F]): KafkaAdminApi[F] =
    KafkaAdminApi[F](adminR, topicName, settings.consumerSettings)

  def admin(topicName: TopicNameL)(implicit F: Async[F]): KafkaAdminApi[F] =
    admin(TopicName(topicName))

  def admin(implicit F: Async[F]): Resource[F, KafkaAdminClient[F]] =
    KafkaAdminClient.resource[F](settings.adminSettings)

  def cherryPick(topicName: TopicName, partition: Int, offset: Long)(implicit F: Async[F]): F[String] =
    admin(topicName).retrieveRecord(partition, offset).flatMap {
      case None => F.raiseError(new Exception("no record"))
      case Some(value) =>
        schemaRegistry.fetchAvroSchema(topicName).flatMap { schemaPair =>
          val pgr = new PullGenericRecord(settings.schemaRegistrySettings, topicName, schemaPair)
          F.fromTry(pgr.toGenericRecord(value).flatMap(gr2Jackson(_)))
        }
    }

  def cherryPick(topicName: TopicNameL, partition: Int, offset: Long)(implicit F: Async[F]): F[String] =
    cherryPick(TopicName(topicName), partition, offset)
}

object KafkaContext {
  def apply[F[_]](settings: KafkaSettings): KafkaContext[F] = new KafkaContext[F](settings)
}
