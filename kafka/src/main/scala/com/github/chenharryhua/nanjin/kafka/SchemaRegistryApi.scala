package com.github.chenharryhua.nanjin.kafka

import cats.Show
import cats.effect.kernel.Sync
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.messages.kafka.NJConsumerRecord
import diffson.*
import diffson.circe.*
import diffson.jsonpatch.Operation
import diffson.jsonpatch.lcsdiff.*
import diffson.lcs.*
import io.circe.*
import io.circe.parser.parse
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaMetadata}
import org.apache.avro.Schema

import scala.jdk.CollectionConverters.*
import scala.util.Try

final private case class SchemaLocation(topicName: TopicName) {
  val keyLoc: String = s"${topicName.value}-key"
  val valLoc: String = s"${topicName.value}-value"
}

final case class AvroSchemaPair(key: Schema, value: Schema) {
  val consumerRecord: Schema = NJConsumerRecord.schema(key, value)
}

object AvroSchemaPair {
  implicit val showAvroSchemaPair: Show[AvroSchemaPair] = _.consumerRecord.toString
}

final case class KvSchemaMetadata(key: Option[SchemaMetadata], value: Option[SchemaMetadata]) {

  private def showKey: String =
    s"""|key schema:
        |id:      ${key.map(_.getId).getOrElse("none")}
        |version: ${key.map(_.getVersion).getOrElse("none")}
        |schema:  ${key.map(_.getSchema).getOrElse("none")}
    """.stripMargin

  private def showValue: String =
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

  override val toString: String =
    s"""
       |compatibility test report of topic($topicName):
       |key:   $keyDescription
       |
       |value: $valueDescription""".stripMargin

  val isCompatible: Boolean = key.flatMap(k => value.map(v => k && v)).fold(_ => false, identity)

  implicit val lcs: Patience[Json] = new Patience[Json]

  private val diffKey: Option[List[Operation[Json]]] = for {
    kafkaKeySchema <- meta.key.flatMap(skm => parse(skm.getSchema).toOption)
    localKeySchema <- parse(keySchema.canonicalString()).toOption
  } yield diff(kafkaKeySchema, localKeySchema).ops

  private val diffVal: Option[List[Operation[Json]]] = for {
    kafkaValSchema <- meta.value.flatMap(skm => parse(skm.getSchema).toOption)
    localValSchema <- parse(valueSchema.canonicalString()).toOption
  } yield diff(kafkaValSchema, localValSchema).ops

  val isIdentical: Boolean = (diffKey, diffVal).mapN(_ ::: _).exists(_.isEmpty)

}

final class SchemaRegistryApi[F[_]](client: CachedSchemaRegistryClient) extends Serializable {

  def metaData(topicName: TopicName)(implicit F: Sync[F]): F[KvSchemaMetadata] = {
    val loc = SchemaLocation(topicName)
    F.delay(
      KvSchemaMetadata(
        Try(client.getLatestSchemaMetadata(loc.keyLoc)).toOption,
        Try(client.getLatestSchemaMetadata(loc.valLoc)).toOption))
  }

  private def kvSchema(topicName: TopicName)(implicit F: Sync[F]): F[(Option[Schema], Option[Schema])] =
    metaData(topicName).map { kv =>
      val ks = kv.key.filter(_.getSchemaType === "AVRO").map(_.getSchema).map(new AvroSchema(_).rawSchema())
      val vs = kv.value.filter(_.getSchemaType === "AVRO").map(_.getSchema).map(new AvroSchema(_).rawSchema())
      (ks, vs)
    }

  def fetchAvroSchema(topicName: TopicName)(implicit F: Sync[F]): F[AvroSchemaPair] =
    kvSchema(topicName).flatMap { case (ks, vs) =>
      (ks, vs).mapN((_, _)) match {
        case Some((k, v)) => F.pure(AvroSchemaPair(k, v))
        case None         => F.raiseError(new Exception(s"unable to retrieve schema for ${topicName.value}"))
      }
    }

  def register(topicName: TopicName, pair: AvroSchemaPair)(implicit
    F: Sync[F]): F[(Option[Int], Option[Int])] = {
    val loc = SchemaLocation(topicName)
    (
      F.delay(client.register(loc.keyLoc, new AvroSchema(pair.key))).attempt.map(_.toOption),
      F.delay(client.register(loc.valLoc, new AvroSchema(pair.value))).attempt.map(_.toOption)).mapN((_, _))
  }

  def register[K, V](topic: TopicDef[K, V])(implicit F: Sync[F]): F[(Option[Int], Option[Int])] =
    register(topic.topicName, topic.schema)

  def delete(topicName: TopicName)(implicit F: Sync[F]): F[(List[Integer], List[Integer])] = {
    val loc = SchemaLocation(topicName)
    (
      F.delay(client.deleteSubject(loc.keyLoc).asScala.toList).attempt.map(_.toOption.sequence.flatten),
      F.delay(client.deleteSubject(loc.valLoc).asScala.toList).attempt.map(_.toOption.sequence.flatten))
      .mapN((_, _))
  }

  def testCompatibility(topicName: TopicName, pair: AvroSchemaPair)(implicit
    F: Sync[F]): F[CompatibilityTestReport] = {
    val loc = SchemaLocation(topicName)
    val ks  = new AvroSchema(pair.key)
    val vs  = new AvroSchema(pair.value)
    (
      F.delay(client.testCompatibility(loc.keyLoc, ks)).attempt.map(_.leftMap(_.getMessage)),
      F.delay(client.testCompatibility(loc.valLoc, vs)).attempt.map(_.leftMap(_.getMessage)),
      metaData(topicName)).mapN((k, v, m) => CompatibilityTestReport(topicName, m, ks, vs, k, v))
  }

  def testCompatibility[K, V](topic: TopicDef[K, V])(implicit F: Sync[F]): F[CompatibilityTestReport] =
    testCompatibility(topic.topicName, topic.schema)
}
