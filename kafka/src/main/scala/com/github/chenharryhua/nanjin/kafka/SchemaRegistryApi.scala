package com.github.chenharryhua.nanjin.kafka

import cats.effect.kernel.Sync
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaMetadata}

import scala.jdk.CollectionConverters.*

final private case class SchemaLocation(topicName: TopicName) {
  val keyLoc: String = s"${topicName.value}-key"
  val valLoc: String = s"${topicName.value}-value"
}

final case class KvSchemaMetadata(key: SchemaMetadata, value: SchemaMetadata)

final class SchemaRegistryApi[F[_]](client: CachedSchemaRegistryClient) extends Serializable {

  def metaData(topicName: TopicName)(implicit F: Sync[F]): F[KvSchemaMetadata] = {
    val loc = SchemaLocation(topicName)
    for {
      key <- F.blocking(client.getLatestSchemaMetadata(loc.keyLoc))
      value <- F.blocking(client.getLatestSchemaMetadata(loc.valLoc))
    } yield KvSchemaMetadata(key, value)
  }

  def fetchAvroSchema(topicName: TopicName)(implicit F: Sync[F]): F[AvroSchemaPair] =
    metaData(topicName).map { kv =>
      val ks = new AvroSchema(kv.key.getSchema).rawSchema()
      val vs = new AvroSchema(kv.value.getSchema).rawSchema()
      AvroSchemaPair(ks, vs)
    }

  def register(topicName: TopicName, pair: AvroSchemaPair)(implicit F: Sync[F]): F[(Int, Int)] = {
    val loc = SchemaLocation(topicName)
    for {
      k <- F.blocking(client.register(loc.keyLoc, new AvroSchema(pair.key)))
      v <- F.blocking(client.register(loc.valLoc, new AvroSchema(pair.value)))
    } yield (k, v)
  }

  def register[K, V](topic: TopicDef[K, V])(implicit F: Sync[F]): F[(Int, Int)] =
    register(topic.topicName, topic.schemaPair)

  def delete(topicName: TopicName)(implicit F: Sync[F]): F[(List[Integer], List[Integer])] = {
    val loc = SchemaLocation(topicName)
    for {
      k <- F.blocking(client.deleteSubject(loc.keyLoc).asScala.toList)
      v <- F.blocking(client.deleteSubject(loc.valLoc).asScala.toList)
    } yield (k, v)
  }
}
