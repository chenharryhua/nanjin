package com.github.chenharryhua.nanjin.kafka

import cats.effect.kernel.Sync
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import io.confluent.kafka.schemaregistry.ParsedSchema
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaMetadata}
import io.confluent.kafka.schemaregistry.json.JsonSchema
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema

import scala.jdk.CollectionConverters.*

sealed trait SchemaRegistryApi[F[_]] {
  def fetchAvroSchema(topicName: TopicName): F[(AvroSchema, AvroSchema)]

  def fetchOptionalAvroSchema[K, V](avroTopic: AvroTopic[K, V]): F[OptionalAvroSchemaPair]
  def fetchOptionalJsonSchema[K, V](jsonTopic: JsonTopic[K, V]): F[OptionalJsonSchemaPair]
  def fetchOptionalProtobufSchema[K, V](protoTopic: ProtoTopic[K, V]): F[OptionalProtobufSchemaPair]

  def register[K, V](topic: KafkaTopic[K, V]): F[RegisteredSchemaID]
  def register(topicName: TopicName, key: ParsedSchema, value: ParsedSchema): F[RegisteredSchemaID]

  def delete(topicName: TopicName): F[(List[Integer], List[Integer])]
}

final private case class SchemaLocation(topicName: TopicName) {
  val keyLoc: String = s"${topicName.name.value}-key"
  val valLoc: String = s"${topicName.name.value}-value"
}

final private class SchemaRegistryApiImpl[F[_]](client: CachedSchemaRegistryClient)(implicit F: Sync[F])
    extends SchemaRegistryApi[F] {

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

  override def fetchAvroSchema(topicName: TopicName): F[(AvroSchema, AvroSchema)] =
    for {
      key <- keyMetaData(topicName)
      value <- valMetaData(topicName)
    } yield (new AvroSchema(key.getSchema), new AvroSchema(value.getSchema))

  override def fetchOptionalAvroSchema[K, V](avroTopic: AvroTopic[K, V]): F[OptionalAvroSchemaPair] =
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

  override def fetchOptionalJsonSchema[K, V](jsonTopic: JsonTopic[K, V]): F[OptionalJsonSchemaPair] =
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

  override def fetchOptionalProtobufSchema[K, V](
    protoTopic: ProtoTopic[K, V]): F[OptionalProtobufSchemaPair] =
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

  def register(topicName: TopicName, key: ParsedSchema, value: ParsedSchema): F[RegisteredSchemaID] = {
    val loc = SchemaLocation(topicName)
    F.blocking {
      RegisteredSchemaID(client.register(loc.keyLoc, key).some, client.register(loc.valLoc, value).some)
    }
  }

  /** register topic's schema. primitive type will not be registered.
    * @return
    *   pair of Schema ID
    */
  override def register[K, V](topic: KafkaTopic[K, V]): F[RegisteredSchemaID] =
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
        } yield RegisteredSchemaID(k, v)

      case ProtoTopic(topicName, pair) =>
        val loc = SchemaLocation(topicName)
        for {
          k <- F.blocking(pair.optionalSchemaPair.key.flatMap { k =>
            if (pair.key.isPrimitive) None else Some(client.register(loc.keyLoc, k))
          })
          v <- F.blocking(pair.optionalSchemaPair.value.flatMap { v =>
            if (pair.value.isPrimitive) None else Some(client.register(loc.valLoc, v))
          })
        } yield RegisteredSchemaID(k, v)

      case JsonTopic(topicName, pair) =>
        val loc = SchemaLocation(topicName)
        for {
          k <- F.blocking(pair.optionalSchemaPair.key.flatMap { k =>
            if (pair.key.isPrimitive) None else Some(client.register(loc.keyLoc, k))
          })
          v <- F.blocking(pair.optionalSchemaPair.value.flatMap { v =>
            if (pair.value.isPrimitive) None else Some(client.register(loc.valLoc, v))
          })
        } yield RegisteredSchemaID(k, v)
    }

  override def delete(topicName: TopicName): F[(List[Integer], List[Integer])] = {
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
