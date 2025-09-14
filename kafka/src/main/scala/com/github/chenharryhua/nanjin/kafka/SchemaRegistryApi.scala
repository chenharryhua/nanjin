package com.github.chenharryhua.nanjin.kafka

import cats.effect.kernel.Sync
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.kafka.{TopicName, TopicNameL}
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaMetadata}

import scala.jdk.CollectionConverters.*

final private case class SchemaLocation(topicName: TopicName) {
  val keyLoc: String = s"${topicName.value}-key"
  val valLoc: String = s"${topicName.value}-value"
}

final case class KvSchemaMetadata(key: SchemaMetadata, value: SchemaMetadata)

final class SchemaRegistryApi[F[_]](client: CachedSchemaRegistryClient) extends Serializable {

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
          Some(new AvroSchema(sm.getSchema).rawSchema())
        })
      value <- valMetaData(topicName).redeem(
        _ => None,
        sm => {
          require(sm.getSchemaType === "AVRO", "value schema is not AVRO")
          Some(new AvroSchema(sm.getSchema).rawSchema())
        })
    } yield new OptionalAvroSchemaPair(key, value)

  def fetchAvroSchema(topicName: TopicName)(implicit F: Sync[F]): F[AvroSchemaPair] =
    for {
      mkv <- metaData(topicName)
      skv <- (mkv.key.getSchemaType === "AVRO", mkv.value.getSchemaType === "AVRO") match {
        case (true, true) =>
          F.pure(
            AvroSchemaPair(
              new AvroSchema(mkv.key.getSchema).rawSchema(),
              new AvroSchema(mkv.value.getSchema).rawSchema()))
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

  def register(topicName: TopicName, pair: AvroSchemaPair)(implicit F: Sync[F]): F[(Int, Int)] = {
    val loc = SchemaLocation(topicName)
    for {
      k <- F
        .blocking(client.register(loc.keyLoc, new AvroSchema(pair.key)))
        .adaptError(ex => new Exception(topicName.value, ex))
      v <- F
        .blocking(client.register(loc.valLoc, new AvroSchema(pair.value)))
        .adaptError(ex => new Exception(topicName.value, ex))
    } yield (k, v)
  }

  def register[K, V](topic: TopicDef[K, V])(implicit F: Sync[F]): F[(Int, Int)] =
    register(topic.topicName, topic.schemaPair)

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
