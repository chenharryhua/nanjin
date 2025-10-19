package com.github.chenharryhua.nanjin.kafka

import cats.effect.kernel.Sync
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaMetadata}
import io.confluent.kafka.schemaregistry.json.JsonSchema
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema

import scala.jdk.CollectionConverters.*

final private case class SchemaLocation(topicName: TopicName) {
  val keyLoc: String = s"${topicName.name.value}-key"
  val valLoc: String = s"${topicName.name.value}-value"
}

final class SchemaRegistryApi[F[_]](client: CachedSchemaRegistryClient) {

  private def keyMetaData(topicName: TopicName)(implicit F: Sync[F]): F[SchemaMetadata] = {
    val loc = SchemaLocation(topicName)
    F.blocking(client.getLatestSchemaMetadata(loc.keyLoc))
  }

  private def valMetaData(topicName: TopicName)(implicit F: Sync[F]): F[SchemaMetadata] = {
    val loc = SchemaLocation(topicName)
    F.blocking(client.getLatestSchemaMetadata(loc.valLoc))
  }

  private def handleError(ex: Throwable): None.type =
    ex match {
      case _: RestClientException => None
      case ex                     => throw ex
    }

  def fetchOptionalAvroSchema(topicName: TopicName)(implicit F: Sync[F]): F[OptionalAvroSchemaPair] =
    for {
      key <- keyMetaData(topicName).redeem(
        handleError,
        sm => {
          require(sm.getSchemaType === "AVRO", "key schema is not AVRO")
          Some(new AvroSchema(sm.getSchema))
        })
      value <- valMetaData(topicName).redeem(
        handleError,
        sm => {
          require(sm.getSchemaType === "AVRO", "value schema is not AVRO")
          Some(new AvroSchema(sm.getSchema))
        }
      )
    } yield OptionalAvroSchemaPair(key, value)

  def fetchOptionalJsonSchema(topicName: TopicName)(implicit F: Sync[F]): F[OptionalJsonSchemaPair] =
    for {
      key <- keyMetaData(topicName).redeem(
        handleError,
        sm => {
          require(sm.getSchemaType === "JSON", "key schema is not JSON")
          Some(new JsonSchema(sm.getSchema))
        })
      value <- valMetaData(topicName).redeem(
        handleError,
        sm => {
          require(sm.getSchemaType === "JSON", "value schema is not JSON")
          Some(new JsonSchema(sm.getSchema))
        }
      )
    } yield OptionalJsonSchemaPair(key, value)

  def fetchOptionalProtobufSchema(topicName: TopicName)(implicit F: Sync[F]): F[OptionalProtobufSchemaPair] =
    for {
      key <- keyMetaData(topicName).redeem(
        handleError,
        sm => {
          require(sm.getSchemaType === "PROTOBUF", "key schema is not PROTOBUF")
          Some(new ProtobufSchema(sm.getSchema))
        }
      )
      value <- valMetaData(topicName).redeem(
        handleError,
        sm => {
          require(sm.getSchemaType === "PROTOBUF", "value schema is not PROTOBUF")
          Some(new ProtobufSchema(sm.getSchema))
        }
      )
    } yield OptionalProtobufSchemaPair(key, value)

  def register[K, V](topic: AvroTopic[K, V])(implicit F: Sync[F]): F[(Option[Int], Option[Int])] = {
    val loc = SchemaLocation(topic.topicName)
    for {
      k <- F
        .blocking(topic.pair.optionalSchemaPair.key.map(k => client.register(loc.keyLoc, k)))
        .adaptError(ex => new Exception(topic.topicName.name.value, ex))
      v <- F
        .blocking(topic.pair.optionalSchemaPair.value.map(v => client.register(loc.valLoc, v)))
        .adaptError(ex => new Exception(topic.topicName.name.value, ex))
    } yield (k, v)
  }

  def delete(topicName: TopicName)(implicit F: Sync[F]): F[(List[Integer], List[Integer])] = {
    val loc = SchemaLocation(topicName)
    for {
      k <- F
        .blocking(client.deleteSubject(loc.keyLoc).asScala.toList)
        .adaptError(ex => new Exception(topicName.name.value, ex))
      v <- F
        .blocking(client.deleteSubject(loc.valLoc).asScala.toList)
        .adaptError(ex => new Exception(topicName.name.value, ex))
    } yield (k, v)
  }
}
