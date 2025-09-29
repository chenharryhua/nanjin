package com.github.chenharryhua.nanjin.kafka

import cats.effect.kernel.Sync
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.kafka.{TopicName, TopicNameL}
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaMetadata}
import io.confluent.kafka.schemaregistry.json.JsonSchema
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema

import scala.jdk.CollectionConverters.*

final private case class SchemaLocation(topicName: TopicName) {
  val keyLoc: String = s"${topicName.value}-key"
  val valLoc: String = s"${topicName.value}-value"
}

final case class KvSchemaMetadata(key: SchemaMetadata, value: SchemaMetadata)

final class SchemaRegistryApi[F[_]](client: CachedSchemaRegistryClient) {

  private def keyMetaData(topicName: TopicName)(implicit F: Sync[F]): F[SchemaMetadata] = {
    val loc = SchemaLocation(topicName)
    F.blocking(client.getLatestSchemaMetadata(loc.keyLoc))
      .adaptError(ex => new Exception(topicName.value, ex))
  }

  private def valMetaData(topicName: TopicName)(implicit F: Sync[F]): F[SchemaMetadata] = {
    val loc = SchemaLocation(topicName)
    F.blocking(client.getLatestSchemaMetadata(loc.valLoc))
      .adaptError(ex => new Exception(topicName.value, ex))
  }

  def metaData(topicName: TopicName)(implicit F: Sync[F]): F[KvSchemaMetadata] =
    for {
      key <- keyMetaData(topicName)
      value <- valMetaData(topicName)
    } yield KvSchemaMetadata(key, value)

  def fetchOptionalAvroSchema(topicName: TopicName)(implicit F: Sync[F]): F[OptionalAvroSchemaPair] =
    for {
      key <- keyMetaData(topicName).redeem(
        _ => None,
        sm => {
          require(sm.getSchemaType === "AVRO", "key schema is not AVRO")
          Some(new AvroSchema(sm.getSchema))
        })
      value <- valMetaData(topicName).redeem(
        _ => None,
        sm => {
          require(sm.getSchemaType === "AVRO", "value schema is not AVRO")
          Some(new AvroSchema(sm.getSchema))
        })
    } yield OptionalAvroSchemaPair(key, value)

  def fetchOptionalJsonSchema(topicName: TopicName)(implicit F: Sync[F]): F[OptionalJsonSchemaPair] =
    for {
      key <- keyMetaData(topicName).redeem(
        _ => None,
        sm => {
          require(sm.getSchemaType === "JSON", "key schema is not JSON")
          Some(new JsonSchema(sm.getSchema))
        })
      value <- valMetaData(topicName).redeem(
        _ => None,
        sm => {
          require(sm.getSchemaType === "JSON", "value schema is not JSON")
          Some(new JsonSchema(sm.getSchema))
        })
    } yield OptionalJsonSchemaPair(key, value)

  def fetchOptionalProtobufSchema(topicName: TopicName)(implicit F: Sync[F]): F[OptionalProtobufSchemaPair] =
    for {
      key <- keyMetaData(topicName).redeem(
        _ => None,
        sm => {
          require(sm.getSchemaType === "PROTOBUF", "key schema is not PROTOBUF")
          Some(new ProtobufSchema(sm.getSchema))
        })
      value <- valMetaData(topicName).redeem(
        _ => None,
        sm => {
          require(sm.getSchemaType === "PROTOBUF", "value schema is not PROTOBUF")
          Some(new ProtobufSchema(sm.getSchema))
        })
    } yield OptionalProtobufSchemaPair(key, value)

  def fetchAvroSchema(topicName: TopicName)(implicit F: Sync[F]): F[AvroSchemaPair] =
    for {
      mkv <- metaData(topicName)
      skv <- (mkv.key.getSchemaType === "AVRO", mkv.value.getSchemaType === "AVRO") match {
        case (true, true) =>
          F.pure(AvroSchemaPair(new AvroSchema(mkv.key.getSchema), new AvroSchema(mkv.value.getSchema)))
        case (false, true) =>
          F.raiseError(new Exception(s"${topicName.value} key is not AVRO"))
        case (true, false) =>
          F.raiseError(new Exception(s"${topicName.value} value is not AVRO"))
        case (false, false) =>
          F.raiseError(new Exception(s"${topicName.value} both key and value are not AVRO"))
      }
    } yield skv

  def fetchAvroSchema(topicName: TopicNameL)(implicit F: Sync[F]): F[AvroSchemaPair] =
    fetchAvroSchema(TopicName(topicName))

  def register[K, V](topic: AvroTopic[K, V])(implicit F: Sync[F]): F[(Option[Int], Option[Int])] = {
    val loc = SchemaLocation(topic.topicName)
    for {
      k <- F
        .blocking(topic.pair.optionalSchemaPair.key.map(k => client.register(loc.keyLoc, k)))
        .adaptError(ex => new Exception(topic.topicName.value, ex))
      v <- F
        .blocking(topic.pair.optionalSchemaPair.value.map(v => client.register(loc.valLoc, v)))
        .adaptError(ex => new Exception(topic.topicName.value, ex))
    } yield (k, v)
  }

  def delete(topicName: TopicName)(implicit F: Sync[F]): F[(List[Integer], List[Integer])] = {
    val loc = SchemaLocation(topicName)
    for {
      k <- F
        .blocking(client.deleteSubject(loc.keyLoc).asScala.toList)
        .adaptError(ex => new Exception(topicName.value, ex))
      v <- F
        .blocking(client.deleteSubject(loc.valLoc).asScala.toList)
        .adaptError(ex => new Exception(topicName.value, ex))
    } yield (k, v)
  }
}
