package com.github.chenharryhua.nanjin.codec

import java.io.File

import cats.implicits._
import com.sksamuel.avro4s.{AvroSchema, SchemaFor}
import diffson._
import diffson.circe._
import diffson.jsonpatch._
import diffson.jsonpatch.lcsdiff._
import diffson.lcs.Patience
import io.circe.optics.JsonPath._
import io.circe.parser._
import io.circe.{Json, ParsingFailure}
import org.apache.avro.SchemaCompatibility.SchemaCompatibilityType
import org.apache.avro.{Schema, SchemaCompatibility}

final case class KafkaAvroSchema(schema: Schema) {
  implicit val lcs: Patience[Json] = new Patience[Json]

  private def cleanupJsonDocument: Json => Json = {
    val noVersion   = root.at("version").set(None)
    val noDoc       = root.at("doc").set(None).andThen(root.fields.each.at("doc").set(None))
    val noJavaClass = root.fields.each.at("java-class").set(None)
    noVersion.andThen(noDoc).andThen(noJavaClass)
  }

  def isSame(other: KafkaAvroSchema): Either[ParsingFailure, JsonPatch[Json]] =
    (parse(schema.toString()), parse(other.schema.toString)).mapN { (f, s) =>
      diff(cleanupJsonDocument(f), cleanupJsonDocument(s))
    }

  def isCompatiable(other: KafkaAvroSchema): Boolean =
    SchemaCompatibility
      .checkReaderWriterCompatibility(schema, other.schema)
      .getResult
      .getCompatibility == SchemaCompatibilityType.COMPATIBLE &&
      SchemaCompatibility
        .checkReaderWriterCompatibility(other.schema, schema)
        .getResult
        .getCompatibility == SchemaCompatibilityType.COMPATIBLE

}

object KafkaAvroSchema {
  val parser: Schema.Parser                = new Schema.Parser
  def apply(str: String): KafkaAvroSchema  = new KafkaAvroSchema(parser.parse(str))
  def apply(file: File): KafkaAvroSchema   = new KafkaAvroSchema(parser.parse(file))
  def apply[A: SchemaFor]: KafkaAvroSchema = new KafkaAvroSchema(AvroSchema[A])
}
