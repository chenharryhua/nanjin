package com.github.chenharryhua.nanjin.kafka.admins

import cats.effect.kernel.Sync
import cats.syntax.traverse.given
import cats.syntax.applicativeError.given
import cats.syntax.eq.given
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import com.github.chenharryhua.nanjin.kafka.{
  OptionalAvroSchemaPair,
  OptionalJsonSchemaPair,
  OptionalProtobufSchemaPair,
  RegisteredSchemaID,
  TopicName
}
import io.confluent.kafka.schemaregistry.ParsedSchema
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.client.{SchemaMetadata, SchemaRegistryClient}
import io.confluent.kafka.schemaregistry.json.JsonSchema
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema

import scala.jdk.CollectionConverters.*

/** Algebra for interacting with Confluent Schema Registry.
  *
  * This API provides:
  *   - schema discovery (required and optional)
  *   - schema registration for Kafka topics
  *   - schema deletion (key/value subjects)
  *
  * ## Subject naming Schemas are resolved using the standard subject naming convention:
  *
  *   - `<topic>-key`
  *   - `<topic>-value`
  *
  * ## Primitive handling For topics whose key and/or value is a primitive type:
  *   - schemas are not registered
  *   - missing schemas may be tolerated when fetching optional schemas
  *
  * ## Error semantics
  *   - Missing required schemas result in `SchemaNotFound`
  *   - Deletion failures are wrapped in `DeleteSchemaException`
  *
  * All interactions with the underlying Schema Registry client are performed in a blocking-safe manner.
  */
sealed trait SchemaRegistryApi[F[_]] {
  def fetchAvroSchema(topicName: TopicName): F[(AvroSchema, AvroSchema)]

  def fetchOptionalAvroSchema(topicName: TopicName): F[OptionalAvroSchemaPair]
  def fetchOptionalJsonSchema(topicName: TopicName): F[OptionalJsonSchemaPair]
  def fetchOptionalProtobufSchema(topicName: TopicName): F[OptionalProtobufSchemaPair]

  def register(
    topicName: TopicName,
    key: Option[ParsedSchema] = None,
    value: Option[ParsedSchema] = None): F[RegisteredSchemaID]

  def delete(topicName: TopicName): F[(List[Integer], List[Integer])]
}

final case class SchemaNotFound(topicName: TopicName, keyOrValue: String, schemaType: String, cause: String)
    extends Exception(s"$schemaType $keyOrValue schema of $topicName can not be found. cause: $cause")
final case class DeleteSchemaException(topicName: TopicName, keyOrValue: String, cause: Throwable)
    extends Exception(cause)

private[kafka] object SchemaRegistryApi {
  def apply[F[_]: Sync](client: SchemaRegistryClient): SchemaRegistryApi[F] =
    new SchemaRegistryApiImpl[F](client)

  final private class SchemaRegistryApiImpl[F[_]](client: SchemaRegistryClient)(using F: Sync[F])
      extends SchemaRegistryApi[F] {

    private val KEY: String = "key"
    private val VALUE: String = "value"

    private case class SchemaLocation(topicName: TopicName) {
      val keyLoc: String = s"${topicName.value}-$KEY"
      val valLoc: String = s"${topicName.value}-$VALUE"
    }

    private def key_meta_data(topicName: TopicName): F[SchemaMetadata] = {
      val loc = SchemaLocation(topicName)
      F.blocking(client.getLatestSchemaMetadata(loc.keyLoc))
    }

    private def val_meta_data(topicName: TopicName): F[SchemaMetadata] = {
      val loc = SchemaLocation(topicName)
      F.blocking(client.getLatestSchemaMetadata(loc.valLoc))
    }

    override def fetchAvroSchema(topicName: TopicName): F[(AvroSchema, AvroSchema)] =
      for {
        key <- key_meta_data(topicName)
        value <- val_meta_data(topicName)
      } yield (new AvroSchema(key.getSchema), new AvroSchema(value.getSchema))

    private def fetch_optional_schema(
      topicName: TopicName,
      schemaType: String): F[(Option[String], Option[String])] =
      for {
        key <- key_meta_data(topicName).attempt.map(_.toOption.filter(_.getSchemaType === schemaType))
        value <- val_meta_data(topicName).attempt.map(_.toOption.filter(_.getSchemaType === schemaType))
      } yield (key.map(_.getSchema), value.map(_.getSchema))

    override def fetchOptionalAvroSchema(topicName: TopicName): F[OptionalAvroSchemaPair] =
      fetch_optional_schema(topicName, "AVRO").map { case (k, v) =>
        val ks = k.map(new AvroSchema(_))
        val vs = v.map(new AvroSchema(_))
        OptionalAvroSchemaPair(ks, vs)
      }

    override def fetchOptionalJsonSchema(topicName: TopicName): F[OptionalJsonSchemaPair] =
      fetch_optional_schema(topicName, "JSON").map { case (k, v) =>
        val ks = k.map(new JsonSchema(_))
        val vs = v.map(new JsonSchema(_))
        OptionalJsonSchemaPair(ks, vs)
      }

    override def fetchOptionalProtobufSchema(topicName: TopicName): F[OptionalProtobufSchemaPair] =
      fetch_optional_schema(topicName, "PROTOBUF").map { case (k, v) =>
        val ks = k.map(new ProtobufSchema(_))
        val vs = v.map(new ProtobufSchema(_))
        OptionalProtobufSchemaPair(ks, vs)
      }

    def register(
      topicName: TopicName,
      key: Option[ParsedSchema] = None,
      value: Option[ParsedSchema] = None): F[RegisteredSchemaID] = {
      val loc = SchemaLocation(topicName)
      F.blocking {
        RegisteredSchemaID(key.map(client.register(loc.keyLoc, _)), value.map(client.register(loc.valLoc, _)))
      }
    }

    override def delete(topicName: TopicName): F[(List[Integer], List[Integer])] = {
      val loc = SchemaLocation(topicName)
      for {
        k <- F
          .blocking(client.deleteSubject(loc.keyLoc))
          .attempt
          .map(_.toOption.traverse(_.asScala.toList).flatten)
        v <- F
          .blocking(client.deleteSubject(loc.valLoc))
          .attempt
          .map(_.toOption.traverse(_.asScala.toList).flatten)
      } yield (k, v)
    }
  }
}
