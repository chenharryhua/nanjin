package com.github.chenharryhua.nanjin.kafka

import cats.Show
import cats.effect.kernel.{Resource, Sync}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaMetadata}
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import org.apache.avro.Schema

import scala.jdk.CollectionConverters.*
import scala.util.Try

final private case class SchemaLocation(topicName: TopicName) {
  val keyLoc: String = s"${topicName.value}-key"
  val valLoc: String = s"${topicName.value}-value"
}

final case class KvSchemaMetadata(key: Option[SchemaMetadata], value: Option[SchemaMetadata]) {

  def showKey: String =
    s"""|key schema:
        |id:      ${key.map(_.getId).getOrElse("none")}
        |version: ${key.map(_.getVersion).getOrElse("none")}
        |schema:  ${key.map(_.getSchema).getOrElse("none")}
    """.stripMargin

  def showValue: String =
    s"""|value schema:
        |id:      ${value.map(_.getId).getOrElse("none")}
        |version: ${value.map(_.getVersion).getOrElse("none")}
        |schema:  ${value.map(_.getSchema).getOrElse("none")}
""".stripMargin

  override def toString: String =
    s"""|key and value schema: 
        |$showKey
        |$showValue
       """.stripMargin

}

object KvSchemaMetadata {
  implicit val showKvSchemaMetadata: Show[KvSchemaMetadata] = _.toString
}

final case class CompatibilityTestReport(
  topicName: TopicName,
  srSettings: SchemaRegistrySettings,
  meta: KvSchemaMetadata,
  keySchema: AvroSchema,
  valueSchema: AvroSchema,
  key: Either[String, Boolean],
  value: Either[String, Boolean]) {

  private val keyDescription: String = key.fold(
    identity,
    if (_) "compatible"
    else
      s"""|incompatible:
          |application:  $keySchema
          |server:       ${meta.key.map(_.getSchema).getOrElse("none")}
          |""".stripMargin
  )

  private val valueDescription: String = value.fold(
    identity,
    if (_) "compatible"
    else
      s"""|incompatible:
          |application:   $valueSchema
          |server:        ${meta.value.map(_.getSchema).getOrElse("none")}
          |""".stripMargin
  )

  val show: String =
    s"""
       |compatibility test report of topic($topicName):
       |key:   $keyDescription
       |
       |value: $valueDescription
       |$srSettings""".stripMargin

  override val toString: String = show

  val isCompatible: Boolean =
    key.flatMap(k => value.map(v => k && v)).fold(_ => false, identity)
}

final class SchemaRegistryApi[F[_]](srs: SchemaRegistrySettings)(implicit F: Sync[F]) extends Serializable {

  private val csrClient: Resource[F, CachedSchemaRegistryClient] =
    Resource.make[F, CachedSchemaRegistryClient](
      F.delay(srs.config.get(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG) match {
        case None => sys.error("schema url is mandatory but not configured")
        case Some(url) =>
          val size: Int = srs.config
            .get(AbstractKafkaSchemaSerDeConfig.MAX_SCHEMAS_PER_SUBJECT_DOC)
            .flatMap(n => Try(n.toInt).toOption)
            .getOrElse(AbstractKafkaSchemaSerDeConfig.MAX_SCHEMAS_PER_SUBJECT_DEFAULT)
          new CachedSchemaRegistryClient(url, size)
      }))(_ => F.pure(()))

  def kvSchema(topicName: TopicName): F[KvSchemaMetadata] = {
    val loc = SchemaLocation(topicName)
    csrClient.use { client =>
      F.delay(
        KvSchemaMetadata(
          Try(client.getLatestSchemaMetadata(loc.keyLoc)).toOption,
          Try(client.getLatestSchemaMetadata(loc.valLoc)).toOption))
    }
  }

  def register(topicName: TopicName, keySchema: Schema, valSchema: Schema): F[(Option[Int], Option[Int])] = {
    val loc = SchemaLocation(topicName)
    csrClient.use { client =>
      (
        F.delay(client.register(loc.keyLoc, new AvroSchema(keySchema))).attempt.map(_.toOption),
        F.delay(client.register(loc.valLoc, new AvroSchema(valSchema))).attempt.map(_.toOption)).mapN((_, _))
    }
  }

  def delete(topicName: TopicName): F[(List[Integer], List[Integer])] = {
    val loc = SchemaLocation(topicName)
    csrClient.use { client =>
      (
        F.delay(client.deleteSubject(loc.keyLoc).asScala.toList).attempt.map(_.toOption.sequence.flatten),
        F.delay(client.deleteSubject(loc.valLoc).asScala.toList).attempt.map(_.toOption.sequence.flatten)).mapN((_, _))
    }
  }

  def testCompatibility(topicName: TopicName, keySchema: Schema, valSchema: Schema): F[CompatibilityTestReport] = {
    val loc = SchemaLocation(topicName)
    csrClient.use { client =>
      val ks = new AvroSchema(keySchema)
      val vs = new AvroSchema(valSchema)
      (
        F.delay(client.testCompatibility(loc.keyLoc, ks)).attempt.map(_.leftMap(_.getMessage)),
        F.delay(client.testCompatibility(loc.valLoc, vs)).attempt.map(_.leftMap(_.getMessage)),
        kvSchema(topicName)).mapN((k, v, m) => CompatibilityTestReport(topicName, srs, m, ks, vs, k, v))
    }
  }
}
