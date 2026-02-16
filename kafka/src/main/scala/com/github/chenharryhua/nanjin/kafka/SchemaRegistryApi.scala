package com.github.chenharryhua.nanjin.kafka

import cats.effect.kernel.Sync
import cats.syntax.applicativeError.catsSyntaxApplicativeError
import cats.syntax.eq.catsSyntaxEq
import cats.syntax.functor.toFunctorOps
import cats.syntax.flatMap.toFlatMapOps
import cats.syntax.monadError.{catsSyntaxMonadError, catsSyntaxMonadErrorRethrow}
import cats.syntax.option.catsSyntaxOptionId
import cats.syntax.show.showInterpolator
import cats.syntax.traverse.toTraverseOps
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import io.confluent.kafka.schemaregistry.ParsedSchema
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaMetadata}
import io.confluent.kafka.schemaregistry.json.JsonSchema
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema
import org.apache.commons.lang3.exception.ExceptionUtils

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

  def fetchOptionalAvroSchema[K, V](avroTopic: AvroTopic[K, V]): F[OptionalAvroSchemaPair]
  def fetchOptionalJsonSchema[K, V](jsonTopic: JsonTopic[K, V]): F[OptionalJsonSchemaPair]
  def fetchOptionalProtobufSchema[K, V](protoTopic: ProtoTopic[K, V]): F[OptionalProtobufSchemaPair]

  def register[K, V](topic: KafkaTopic[K, V]): F[RegisteredSchemaID]
  def register(topicName: TopicName, key: ParsedSchema, value: ParsedSchema): F[RegisteredSchemaID]

  def delete(topicName: TopicName): F[(List[Integer], List[Integer])]
}

final case class SchemaNotFound(topicName: TopicName, keyOrValue: String, schemaType: String, cause: String)
    extends Exception(show"$schemaType $keyOrValue schema of $topicName can not be found. cause: $cause")
final case class DeleteSchemaException(topicName: TopicName, keyOrValue: String, cause: Throwable)
    extends Exception(cause)

final private class SchemaRegistryApiImpl[F[_]](client: CachedSchemaRegistryClient)(implicit F: Sync[F])
    extends SchemaRegistryApi[F] {

  private val KEY: String = "key"
  private val VALUE: String = "value"

  private case class SchemaLocation(topicName: TopicName) {
    val keyLoc: String = s"${topicName.name.value}-$KEY"
    val valLoc: String = s"${topicName.name.value}-$VALUE"
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
    schemaType: String,
    primKey: Boolean,
    primVal: Boolean): F[(Option[String], Option[String])] =
    for {
      key <- key_meta_data(topicName).attempt.map {
        case Left(ex) =>
          if (primKey) Right(None)
          else Left(SchemaNotFound(topicName, KEY, schemaType, ExceptionUtils.getMessage(ex)))
        case Right(sm) =>
          if (sm.getSchemaType === schemaType) Right(Some(sm.getSchema))
          else
            Left(SchemaNotFound(topicName, KEY, schemaType, s"registered is not a $schemaType schema"))
      }.rethrow
      value <- val_meta_data(topicName).attempt.map {
        case Left(ex) =>
          if (primVal) Right(None)
          else Left(SchemaNotFound(topicName, VALUE, schemaType, ExceptionUtils.getMessage(ex)))
        case Right(sm) =>
          if (sm.getSchemaType === schemaType) Right(Some(sm.getSchema))
          else
            Left(SchemaNotFound(topicName, VALUE, schemaType, s"registered is not a $schemaType schema"))
      }.rethrow
    } yield (key, value)

  override def fetchOptionalAvroSchema[K, V](avroTopic: AvroTopic[K, V]): F[OptionalAvroSchemaPair] =
    fetch_optional_schema(
      avroTopic.topicName,
      "AVRO",
      avroTopic.pair.key.isPrimitive,
      avroTopic.pair.value.isPrimitive).map { case (k, v) =>
      val ks = k.map(new AvroSchema(_))
      val vs = v.map(new AvroSchema(_))
      OptionalAvroSchemaPair(ks, vs)
    }

  override def fetchOptionalJsonSchema[K, V](jsonTopic: JsonTopic[K, V]): F[OptionalJsonSchemaPair] =
    fetch_optional_schema(
      jsonTopic.topicName,
      "JSON",
      jsonTopic.pair.key.isPrimitive,
      jsonTopic.pair.value.isPrimitive).map { case (k, v) =>
      val ks = k.map(new JsonSchema(_))
      val vs = v.map(new JsonSchema(_))
      OptionalJsonSchemaPair(ks, vs)
    }

  override def fetchOptionalProtobufSchema[K, V](
    protoTopic: ProtoTopic[K, V]): F[OptionalProtobufSchemaPair] =
    fetch_optional_schema(
      protoTopic.topicName,
      "PROTOBUF",
      protoTopic.pair.key.isPrimitive,
      protoTopic.pair.value.isPrimitive).map { case (k, v) =>
      val ks = k.map(new ProtobufSchema(_))
      val vs = v.map(new ProtobufSchema(_))
      OptionalProtobufSchemaPair(ks, vs)
    }

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
  override def register[K, V](topic: KafkaTopic[K, V]): F[RegisteredSchemaID] = {
    val loc = SchemaLocation(topic.topicName)
    val (keySchema: Option[ParsedSchema], valSchema: Option[ParsedSchema]) = topic match {
      case AvroTopic(_, pair) =>
        val k = if (pair.key.isPrimitive) None else pair.key.avroSchema
        val v = if (pair.value.isPrimitive) None else pair.value.avroSchema
        k -> v
      case ProtoTopic(_, pair) =>
        val k = if (pair.key.isPrimitive) None else pair.key.protobufSchema
        val v = if (pair.value.isPrimitive) None else pair.value.protobufSchema
        k -> v
      case JsonTopic(_, pair) =>
        val k = if (pair.key.isPrimitive) None else pair.key.jsonSchema
        val v = if (pair.value.isPrimitive) None else pair.value.jsonSchema
        k -> v
    }
    for {
      k <- keySchema.traverse(ps => F.blocking(client.register(loc.keyLoc, ps)))
      v <- valSchema.traverse(ps => F.blocking(client.register(loc.valLoc, ps)))
    } yield RegisteredSchemaID(k, v)
  }

  override def delete(topicName: TopicName): F[(List[Integer], List[Integer])] = {
    val loc = SchemaLocation(topicName)
    for {
      k <- F
        .blocking(client.deleteSubject(loc.keyLoc).asScala.toList)
        .adaptError(ex => DeleteSchemaException(topicName, KEY, ex))
      v <- F
        .blocking(client.deleteSubject(loc.valLoc).asScala.toList)
        .adaptError(ex => DeleteSchemaException(topicName, VALUE, ex))
    } yield (k, v)
  }
}
