package com.github.chenharryhua.nanjin.kafka.admins

import cats.effect.kernel.Sync
import cats.syntax.applicativeError.given
import cats.syntax.eq.given
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import cats.syntax.traverse.given
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

/** Confluent Schema Registry operations scoped to a single Kafka topic.
  *
  * An instance is bound to one topic at construction (obtain one via `KafkaContext.schemaRegistry(topic)`),
  * so the methods take no topic argument. Key and value schemas are addressed under the standard subject
  * names `<topic>-key` and `<topic>-value`.
  *
  * ==Optional vs required==
  * `fetchAvroSchema` requires both key and value schemas to be present. The `fetchOptional*` variants return
  * `None` for a side whose schema is absent or is not of the requested type, which suits topics whose key
  * and/or value is a primitive (and therefore has no registered schema).
  *
  * All calls run on the blocking pool, since the underlying Confluent client is synchronous.
  */
sealed trait TopicSchemaRegistry[F[_]] {

  /** Fetch the latest key and value Avro schemas; both must be present. */
  def fetchAvroSchema: F[(AvroSchema, AvroSchema)]

  /** Fetch the latest key/value Avro schemas, each `None` if absent or not Avro. */
  def fetchOptionalAvroSchema: F[OptionalAvroSchemaPair]

  /** Fetch the latest key/value JSON schemas, each `None` if absent or not JSON. */
  def fetchOptionalJsonSchema: F[OptionalJsonSchemaPair]

  /** Fetch the latest key/value Protobuf schemas, each `None` if absent or not Protobuf. */
  def fetchOptionalProtobufSchema: F[OptionalProtobufSchemaPair]

  /** Register key and/or value schemas for this topic, returning the assigned registry ids.
    *
    * @param key
    *   schema to register under the `-key` subject, or `None` to skip
    * @param value
    *   schema to register under the `-value` subject, or `None` to skip
    */
  def register(key: Option[ParsedSchema] = None, value: Option[ParsedSchema] = None): F[RegisteredSchemaId]

  /** Delete both the `-key` and `-value` subjects for this topic, returning the deleted schema-id lists (key,
    * value). Missing subjects yield empty lists rather than failing.
    */
  def delete: F[(List[Integer], List[Integer])]
}

final case class SchemaNotFound(topicName: TopicName, keyOrValue: String, schemaType: String, cause: String)
    extends Exception(s"$schemaType $keyOrValue schema of $topicName can not be found. cause: $cause")
final case class DeleteSchemaException(topicName: TopicName, keyOrValue: String, cause: Throwable)
    extends Exception(cause)

private[kafka] object TopicSchemaRegistry {
  def apply[F[_]: Sync](client: SchemaRegistryClient, topicName: TopicName): TopicSchemaRegistry[F] =
    new TopicSchemaRegistryImpl[F](client, topicName)

  final private class TopicSchemaRegistryImpl[F[_]] private[TopicSchemaRegistry] (
    client: SchemaRegistryClient,
    topicName: TopicName)(using F: Sync[F])
      extends TopicSchemaRegistry[F] {

    private val key_loc: String = s"${topicName.value}-key"
    private val val_loc: String = s"${topicName.value}-value"

    private val key_meta_data: F[SchemaMetadata] =
      F.blocking(client.getLatestSchemaMetadata(key_loc))

    private val val_meta_data: F[SchemaMetadata] =
      F.blocking(client.getLatestSchemaMetadata(val_loc))

    override val fetchAvroSchema: F[(AvroSchema, AvroSchema)] =
      for {
        key <- key_meta_data
        value <- val_meta_data
      } yield (new AvroSchema(key.getSchema), new AvroSchema(value.getSchema))

    private def fetch_optional_schema(schemaType: String): F[(Option[String], Option[String])] =
      for {
        key <- key_meta_data.attempt.map(_.toOption.filter(_.getSchemaType === schemaType))
        value <- val_meta_data.attempt.map(_.toOption.filter(_.getSchemaType === schemaType))
      } yield (key.map(_.getSchema), value.map(_.getSchema))

    override val fetchOptionalAvroSchema: F[OptionalAvroSchemaPair] =
      fetch_optional_schema("AVRO").map { case (k, v) =>
        val ks = k.map(new AvroSchema(_))
        val vs = v.map(new AvroSchema(_))
        OptionalAvroSchemaPair(ks, vs)
      }

    override val fetchOptionalJsonSchema: F[OptionalJsonSchemaPair] =
      fetch_optional_schema("JSON").map { case (k, v) =>
        val ks = k.map(new JsonSchema(_))
        val vs = v.map(new JsonSchema(_))
        OptionalJsonSchemaPair(ks, vs)
      }

    override val fetchOptionalProtobufSchema: F[OptionalProtobufSchemaPair] =
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

    override val delete: F[(List[Integer], List[Integer])] =
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
