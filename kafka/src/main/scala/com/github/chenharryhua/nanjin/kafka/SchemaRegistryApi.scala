package com.github.chenharryhua.nanjin.kafka

import cats.effect.kernel.Sync
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaMetadata}
import io.confluent.kafka.schemaregistry.json.JsonSchema
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema

import scala.jdk.CollectionConverters.*

final private case class SchemaLocation(topicName: TopicName) {
  val keyLoc: String = s"${topicName.name.value}-key"
  val valLoc: String = s"${topicName.name.value}-value"
}

final class SchemaRegistryApi[F[_]](client: CachedSchemaRegistryClient)(implicit F: Sync[F]) {

  private def keyMetaData(topicName: TopicName): F[SchemaMetadata] = {
    val loc = SchemaLocation(topicName)
    F.blocking(client.getLatestSchemaMetadata(loc.keyLoc))
  }

  private def valMetaData(topicName: TopicName): F[SchemaMetadata] = {
    val loc = SchemaLocation(topicName)
    F.blocking(client.getLatestSchemaMetadata(loc.valLoc))
  }

  private def handleError(isPrimitive: Boolean)(throwable: Throwable): None.type =
    if (isPrimitive) None else throw throwable

  def fetchOptionalAvroSchema[K, V](avroTopic: AvroTopic[K, V]): F[OptionalAvroSchemaPair] =
    for {
      key <- keyMetaData(avroTopic.topicName).redeem(
        handleError(avroTopic.pair.key.isPrimitive),
        sm => {
          require(sm.getSchemaType === "AVRO", "key schema is not AVRO")
          Some(new AvroSchema(sm.getSchema))
        })
      value <- valMetaData(avroTopic.topicName).redeem(
        handleError(avroTopic.pair.value.isPrimitive),
        sm => {
          require(sm.getSchemaType === "AVRO", "value schema is not AVRO")
          Some(new AvroSchema(sm.getSchema))
        }
      )
    } yield OptionalAvroSchemaPair(key, value)

  def fetchOptionalJsonSchema[K, V](jsonTopic: JsonTopic[K, V]): F[OptionalJsonSchemaPair] =
    for {
      key <- keyMetaData(jsonTopic.topicName).redeem(
        handleError(jsonTopic.pair.key.isPrimitive),
        sm => {
          require(sm.getSchemaType === "JSON", "key schema is not JSON")
          Some(new JsonSchema(sm.getSchema))
        })
      value <- valMetaData(jsonTopic.topicName).redeem(
        handleError(jsonTopic.pair.value.isPrimitive),
        sm => {
          require(sm.getSchemaType === "JSON", "value schema is not JSON")
          Some(new JsonSchema(sm.getSchema))
        }
      )
    } yield OptionalJsonSchemaPair(key, value)

  def fetchOptionalProtobufSchema[K, V](protoTopic: ProtoTopic[K, V]): F[OptionalProtobufSchemaPair] =
    for {
      key <- keyMetaData(protoTopic.topicName).redeem(
        handleError(protoTopic.pair.key.isPrimitive),
        sm => {
          require(sm.getSchemaType === "PROTOBUF", "key schema is not PROTOBUF")
          Some(new ProtobufSchema(sm.getSchema))
        }
      )
      value <- valMetaData(protoTopic.topicName).redeem(
        handleError(protoTopic.pair.value.isPrimitive),
        sm => {
          require(sm.getSchemaType === "PROTOBUF", "value schema is not PROTOBUF")
          Some(new ProtobufSchema(sm.getSchema))
        }
      )
    } yield OptionalProtobufSchemaPair(key, value)

  /** register topic's schema. primitive type will not be registered.
    * @return
    */
  def register[K, V](topic: KafkaTopic[K, V]): F[(Option[Int], Option[Int])] =
    topic match {
      case AvroTopic(topicName, pair) =>
        val loc = SchemaLocation(topicName)
        for {
          k <- F.blocking(pair.optionalSchemaPair.key.flatMap { k =>
            if (pair.key.isPrimitive) None else Some(client.register(loc.keyLoc, k))
          })
          v <- F.blocking(pair.optionalSchemaPair.value.flatMap { v =>
            if (pair.value.isPrimitive) None else Some(client.register(loc.valLoc, v))
          })
        } yield (k, v)

      case ProtoTopic(topicName, pair) =>
        val loc = SchemaLocation(topicName)
        for {
          k <- F.blocking(pair.optionalSchemaPair.key.flatMap { k =>
            if (pair.key.isPrimitive) None else Some(client.register(loc.keyLoc, k))
          })
          v <- F.blocking(pair.optionalSchemaPair.value.flatMap { v =>
            if (pair.value.isPrimitive) None else Some(client.register(loc.valLoc, v))
          })
        } yield (k, v)

      case JsonTopic(topicName, pair) =>
        val loc = SchemaLocation(topicName)
        for {
          k <- F.blocking(pair.optionalSchemaPair.key.flatMap { k =>
            if (pair.key.isPrimitive) None else Some(client.register(loc.keyLoc, k))
          })
          v <- F.blocking(pair.optionalSchemaPair.value.flatMap { v =>
            if (pair.value.isPrimitive) None else Some(client.register(loc.valLoc, v))
          })
        } yield (k, v)
    }

  def delete(topicName: TopicName): F[(List[Integer], List[Integer])] = {
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
