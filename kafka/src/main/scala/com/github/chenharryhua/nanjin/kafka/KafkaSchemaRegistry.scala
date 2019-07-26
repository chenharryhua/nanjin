package com.github.chenharryhua.nanjin.kafka

import avrohugger.Generator
import avrohugger.format.Standard
import cats.Show
import cats.effect.Sync
import cats.implicits._
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaMetadata}

import scala.collection.JavaConverters._
import scala.util.Try

final case class KvSchemaMetadata(key: Option[SchemaMetadata], value: Option[SchemaMetadata]) {
  private def genCaseClass(s: SchemaMetadata): Option[String] =
    Try(Generator(Standard).stringToStrings(s.getSchema).mkString("\n")).toEither.toOption

  def show: String =
    s"""
       |key schema
       |id:      ${key.map(_.getId).getOrElse("none")}
       |version: ${key.map(_.getVersion).getOrElse("none")}
       |schema:  ${key.map(_.getSchema).getOrElse("none")}
       |scala:   ${key.flatMap(genCaseClass).getOrElse("none")}
       |value schema
       |id:      ${value.map(_.getId).getOrElse("none")}
       |version: ${value.map(_.getVersion).getOrElse("none")}
       |schema:  ${value.map(_.getSchema).getOrElse("none")}
       |scala:   ${value.flatMap(genCaseClass).getOrElse("none")}
       """.stripMargin
}

object KvSchemaMetadata {
  implicit val showKvSchemaMetadata: Show[KvSchemaMetadata] = _.show
}

trait KafkaSchemaRegistry[F[_]] extends Serializable {
  def delete: F[(List[Integer], List[Integer])]
  def register[K: SerdeOf, V: SerdeOf]: F[(Option[Int], Option[Int])]
  def latestMeta: F[KvSchemaMetadata]
  def testCompatibility[K: SerdeOf, V: SerdeOf]: F[(Boolean, Boolean)]
}

object KafkaSchemaRegistry {

  def apply[F[_]: Sync](
    srClient: CachedSchemaRegistryClient,
    topicName: KafkaTopicName): KafkaSchemaRegistry[F] =
    new KafkaSchemaRegistryImpl(srClient, topicName)

  final private class KafkaSchemaRegistryImpl[F[_]: Sync](
    srClient: CachedSchemaRegistryClient,
    topicName: KafkaTopicName
  ) extends KafkaSchemaRegistry[F] {

    override def delete: F[(List[Integer], List[Integer])] = {
      val deleteKey =
        Sync[F]
          .delay(srClient.deleteSubject(topicName.keySchemaLoc).asScala.toList)
          .handleError(_ => Nil)
      val deleteValue =
        Sync[F]
          .delay(
            srClient.deleteSubject(topicName.valueSchemaLoc).asScala.toList
          )
          .handleError(_ => Nil)
      (deleteKey, deleteValue).mapN((_, _))
    }

    override def register[K: SerdeOf, V: SerdeOf]: F[(Option[Int], Option[Int])] =
      (
        Sync[F]
          .delay(srClient.register(topicName.keySchemaLoc, SerdeOf[K].schema))
          .attempt
          .map(_.toOption),
        Sync[F]
          .delay(srClient.register(topicName.valueSchemaLoc, SerdeOf[V].schema))
          .attempt
          .map(_.toOption)
      ).mapN((_, _))

    override def testCompatibility[K: SerdeOf, V: SerdeOf]: F[(Boolean, Boolean)] =
      (
        Sync[F]
          .delay(
            srClient.testCompatibility(topicName.keySchemaLoc, SerdeOf[K].schema)
          )
          .attempt
          .map(_.fold(_ => false, identity)),
        Sync[F]
          .delay(
            srClient.testCompatibility(topicName.valueSchemaLoc, SerdeOf[V].schema)
          )
          .attempt
          .map(_.fold(_ => false, identity))
      ).mapN((_, _))

    override def latestMeta: F[KvSchemaMetadata] =
      (
        Sync[F]
          .delay(srClient.getLatestSchemaMetadata(topicName.keySchemaLoc))
          .attempt
          .map(_.toOption),
        Sync[F]
          .delay(srClient.getLatestSchemaMetadata(topicName.valueSchemaLoc))
          .attempt
          .map(_.toOption)
      ).mapN(KvSchemaMetadata(_, _))
  }
}
