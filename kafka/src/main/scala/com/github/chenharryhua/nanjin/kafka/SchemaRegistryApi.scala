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

  private def enhanceException[A](topicName: TopicName)(
    throwable: Either[Throwable, A]): Either[Exception, A] =
    throwable.leftMap { err =>
      val ex = new Exception(topicName.value)
      ex.addSuppressed(err)
      ex
    }

  def metaData(topicName: TopicName)(implicit F: Sync[F]): F[KvSchemaMetadata] = {
    val loc = SchemaLocation(topicName)
    for {
      key <- F
        .blocking(client.getLatestSchemaMetadata(loc.keyLoc))
        .attempt
        .map(enhanceException(topicName))
        .rethrow
      value <- F
        .blocking(client.getLatestSchemaMetadata(loc.valLoc))
        .attempt
        .map(enhanceException(topicName))
        .rethrow
    } yield KvSchemaMetadata(key, value)
  }

  def fetchAvroSchema(topicName: TopicName)(implicit F: Sync[F]): F[AvroSchemaPair] =
    for {
      mkv <- metaData(topicName)
      skv <- (mkv.key.getSchemaType === "AVRO", mkv.value.getSchemaType === "AVRO") match {
        case (true, true) =>
          F.pure(
            AvroSchemaPair(
              new AvroSchema(mkv.key.getSchema).rawSchema(),
              new AvroSchema(mkv.value.getSchema).rawSchema()))
        case (false, true)  => F.raiseError(new Exception(s"$topicName key is not AVRO"))
        case (true, false)  => F.raiseError(new Exception(s"$topicName value is not AVRO"))
        case (false, false) => F.raiseError(new Exception(s"$topicName both key and value are not AVRO"))
      }
    } yield skv

  def register(topicName: TopicName, pair: AvroSchemaPair)(implicit F: Sync[F]): F[(Int, Int)] = {
    val loc = SchemaLocation(topicName)
    for {
      k <- F
        .blocking(client.register(loc.keyLoc, new AvroSchema(pair.key)))
        .attempt
        .map(enhanceException(topicName))
        .rethrow
      v <- F
        .blocking(client.register(loc.valLoc, new AvroSchema(pair.value)))
        .attempt
        .map(enhanceException(topicName))
        .rethrow
    } yield (k, v)
  }

  def register[K, V](topic: TopicDef[K, V])(implicit F: Sync[F]): F[(Int, Int)] =
    register(topic.topicName, topic.schemaPair)

  def delete(topicName: TopicName)(implicit F: Sync[F]): F[(List[Integer], List[Integer])] = {
    val loc = SchemaLocation(topicName)
    for {
      k <- F
        .blocking(client.deleteSubject(loc.keyLoc).asScala.toList)
        .attempt
        .map(enhanceException(topicName))
        .rethrow
      v <- F
        .blocking(client.deleteSubject(loc.valLoc).asScala.toList)
        .attempt
        .map(enhanceException(topicName))
        .rethrow
    } yield (k, v)
  }
}
