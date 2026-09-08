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
  RegisteredSchemaId,
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
sealed trait TopicSchemaRegistry[F[_]] {
  def fetchAvroSchema: F[(AvroSchema, AvroSchema)]

  def fetchOptionalAvroSchema: F[OptionalAvroSchemaPair]
  def fetchOptionalJsonSchema: F[OptionalJsonSchemaPair]
  def fetchOptionalProtobufSchema: F[OptionalProtobufSchemaPair]

  def register(key: Option[ParsedSchema] = None, value: Option[ParsedSchema] = None): F[RegisteredSchemaId]

  def delete: F[(List[Integer], List[Integer])]
}

final case class SchemaNotFound(topicName: TopicName, keyOrValue: String, schemaType: String, cause: String)
    extends Exception(s"$schemaType $keyOrValue schema of $topicName can not be found. cause: $cause")
final case class DeleteSchemaException(topicName: TopicName, keyOrValue: String, cause: Throwable)
    extends Exception(cause)

private[kafka] object TopicSchemaRegistry {
  def apply[F[_]: Sync](client: SchemaRegistryClient, topicName: TopicName): TopicSchemaRegistry[F] =
    new TopicSchemaRegistryImpl[F](client, topicName)

  final private class TopicSchemaRegistryImpl[F[_]](client: SchemaRegistryClient, topicName: TopicName)(using
    F: Sync[F])
      extends TopicSchemaRegistry[F] {

    private val key_loc: String = s"${topicName.value}-key"
    private val val_loc: String = s"${topicName.value}-value"

    private def key_meta_data: F[SchemaMetadata] =
      F.blocking(client.getLatestSchemaMetadata(key_loc))

    private def val_meta_data: F[SchemaMetadata] =
      F.blocking(client.getLatestSchemaMetadata(val_loc))

    override def fetchAvroSchema: F[(AvroSchema, AvroSchema)] =
      for {
        key <- key_meta_data
        value <- val_meta_data
      } yield (new AvroSchema(key.getSchema), new AvroSchema(value.getSchema))

    private def fetch_optional_schema(schemaType: String): F[(Option[String], Option[String])] =
      for {
        key <- key_meta_data.attempt.map(_.toOption.filter(_.getSchemaType === schemaType))
        value <- val_meta_data.attempt.map(_.toOption.filter(_.getSchemaType === schemaType))
      } yield (key.map(_.getSchema), value.map(_.getSchema))

    override def fetchOptionalAvroSchema: F[OptionalAvroSchemaPair] =
      fetch_optional_schema("AVRO").map { case (k, v) =>
        val ks = k.map(new AvroSchema(_))
        val vs = v.map(new AvroSchema(_))
        OptionalAvroSchemaPair(ks, vs)
      }

    override def fetchOptionalJsonSchema: F[OptionalJsonSchemaPair] =
      fetch_optional_schema("JSON").map { case (k, v) =>
        val ks = k.map(new JsonSchema(_))
        val vs = v.map(new JsonSchema(_))
        OptionalJsonSchemaPair(ks, vs)
      }

    override def fetchOptionalProtobufSchema: F[OptionalProtobufSchemaPair] =
      fetch_optional_schema("PROTOBUF").map { case (k, v) =>
        val ks = k.map(new ProtobufSchema(_))
        val vs = v.map(new ProtobufSchema(_))
        OptionalProtobufSchemaPair(ks, vs)
      }

    def register(
      key: Option[ParsedSchema] = None,
      value: Option[ParsedSchema] = None): F[RegisteredSchemaId] =
      F.blocking {
        RegisteredSchemaId(key.map(client.register(key_loc, _)), value.map(client.register(val_loc, _)))
      }

    override def delete: F[(List[Integer], List[Integer])] =
      for {
        k <- F
          .blocking(client.deleteSubject(key_loc))
          .attempt
          .map(_.toOption.traverse(_.asScala.toList).flatten)
        v <- F
          .blocking(client.deleteSubject(val_loc))
          .attempt
          .map(_.toOption.traverse(_.asScala.toList).flatten)
      } yield (k, v)
  }
}
