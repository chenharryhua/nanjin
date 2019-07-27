package com.github.chenharryhua.nanjin.kafka

import avrohugger.Generator
import avrohugger.format.Standard
import cats.Show
import cats.effect.Sync
import cats.implicits._
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaMetadata}
import org.apache.avro.Schema

import scala.collection.JavaConverters._
import scala.util.Try

final case class KvSchemaMetadata(key: Option[SchemaMetadata], value: Option[SchemaMetadata]) {
  private def genCaseClass(s: SchemaMetadata): Option[String] =
    Try(Generator(Standard).stringToStrings(s.getSchema).mkString("\n")).toEither.toOption

  def showKey: String =
    s"""|key schema
        |id:      ${key.map(_.getId).getOrElse("none")}
        |version: ${key.map(_.getVersion).getOrElse("none")}
        |schema:  ${key.map(_.getSchema).getOrElse("none")}
        |scala:   ${key.flatMap(genCaseClass).getOrElse("none")}""".stripMargin

  def showValue: String =
    s"""|value schema
        |id:      ${value.map(_.getId).getOrElse("none")}
        |version: ${value.map(_.getVersion).getOrElse("none")}
        |schema:  ${value.map(_.getSchema).getOrElse("none")}
        |scala:   ${value.flatMap(genCaseClass).getOrElse("none")}""".stripMargin

  def show: String =
    s"""|key and value schema 
        |$showKey
        |$showValue
       """.stripMargin
}

object KvSchemaMetadata {
  implicit val showKvSchemaMetadata: Show[KvSchemaMetadata] = _.show
}

final case class CompatibilityTestReport(
  topicName: KafkaTopicName,
  meta: KvSchemaMetadata,
  keySchema: Schema,
  valueSchema: Schema,
  key: Either[String, Boolean],
  value: Either[String, Boolean]) {

  private val keyDescription: String = key.fold(
    identity,
    if (_) "compatible"
    else
      s"""|incompatible
          |application:  $keySchema
          |server:       ${meta.key.map(_.getSchema).getOrElse("none")}
          |""".stripMargin
  )

  private val valueDescription: String = value.fold(
    identity,
    if (_) "compatible"
    else
      s"""|incompatible
          |application:   $valueSchema
          |server:        ${meta.value.map(_.getSchema).getOrElse("none")}
          |""".stripMargin
  )

  val show: String =
    s"""
       |compatibility test report of topic(${topicName.value}):
       |key:   $keyDescription
       |
       |value: $valueDescription
       |""".stripMargin

  override val toString: String = show

  val isCompatible: Boolean =
    key.flatMap(k => value.map(v => k && v)).fold(_ => false, identity)
}

trait KafkaSchemaRegistry[F[_]] extends Serializable {
  def delete: F[(List[Integer], List[Integer])]
  def register: F[(Option[Int], Option[Int])]
  def latestMeta: F[KvSchemaMetadata]
  def testCompatibility: F[CompatibilityTestReport]
}

object KafkaSchemaRegistry {

  def apply[F[_]: Sync](
    srClient: CachedSchemaRegistryClient,
    topicName: KafkaTopicName,
    keySchema: Schema,
    valueSchema: Schema): KafkaSchemaRegistry[F] =
    new KafkaSchemaRegistryImpl(srClient, topicName, keySchema, valueSchema)

  final private class KafkaSchemaRegistryImpl[F[_]: Sync](
    srClient: CachedSchemaRegistryClient,
    topicName: KafkaTopicName,
    keySchema: Schema,
    valueSchema: Schema
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

    override def register: F[(Option[Int], Option[Int])] =
      (
        Sync[F].delay(srClient.register(topicName.keySchemaLoc, keySchema)).attempt.map(_.toOption),
        Sync[F]
          .delay(srClient.register(topicName.valueSchemaLoc, valueSchema))
          .attempt
          .map(_.toOption)
      ).mapN((_, _))

    override def testCompatibility: F[CompatibilityTestReport] =
      (
        Sync[F]
          .delay(
            srClient.testCompatibility(topicName.keySchemaLoc, keySchema)
          )
          .attempt
          .map(_.leftMap(_.getMessage)),
        Sync[F]
          .delay(
            srClient.testCompatibility(topicName.valueSchemaLoc, valueSchema)
          )
          .attempt
          .map(_.leftMap(_.getMessage)),
        latestMeta
      ).mapN((k, v, meta) => CompatibilityTestReport(topicName, meta, keySchema, valueSchema, k, v))

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
