package com.github.chenharryhua.nanjin.codec

import java.io.{File, InputStream}

import cats.implicits._
import com.sksamuel.avro4s.{AvroSchema, SchemaFor, Decoder => AvroDecoder, Encoder => AvroEncoder}
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

final case class ManualAvroSchema[A: SchemaFor](schema: Schema)(
  implicit
  val decoder: AvroDecoder[A],
  val encoder: AvroEncoder[A]) {

  implicit val lcs: Patience[Json] = new Patience[Json]

  private val inferredSchema: Schema = AvroSchema[A]

  private def cleanupJsonDocument: Json => Json = {
    val noVersion   = root.at("version").set(None)
    val noDoc       = root.at("doc").set(None).andThen(root.fields.each.at("doc").set(None))
    val noJavaClass = root.fields.each.at("java-class").set(None)
    noVersion.andThen(noDoc).andThen(noJavaClass)
  }

  val isSame: Either[ParsingFailure, JsonPatch[Json]] =
    (parse(schema.toString()), parse(inferredSchema.toString)).mapN { (input, inferred) =>
      diff(cleanupJsonDocument(input), cleanupJsonDocument(inferred))
    }

  def isCompatiable: Boolean =
    SchemaCompatibility
      .checkReaderWriterCompatibility(schema, inferredSchema)
      .getResult
      .getCompatibility == SchemaCompatibilityType.COMPATIBLE &&
      SchemaCompatibility
        .checkReaderWriterCompatibility(inferredSchema, schema)
        .getResult
        .getCompatibility == SchemaCompatibilityType.COMPATIBLE

  require(
    isSame.exists(_.ops.isEmpty),
    s"""
       |input schema is not semantically identical to the inferred schema. 
       |input schema:
       |${schema.toString()}
       |inferred schema:
       |${inferredSchema.toString()}
       |diff:
       |$isSame
    """.stripMargin
  )
}

object ManualAvroSchema {

  def apply[A: AvroDecoder: AvroEncoder: SchemaFor](stringSchema: String): ManualAvroSchema[A] = {
    val parser: Schema.Parser = new Schema.Parser
    ManualAvroSchema[A](parser.parse(stringSchema))
  }

  def apply[A: AvroDecoder: AvroEncoder: SchemaFor](file: File): ManualAvroSchema[A] = {
    val parser: Schema.Parser = new Schema.Parser
    ManualAvroSchema[A](parser.parse(file))
  }

  def apply[A: AvroDecoder: AvroEncoder: SchemaFor](is: InputStream): ManualAvroSchema[A] = {
    val parser: Schema.Parser = new Schema.Parser
    ManualAvroSchema[A](parser.parse(is))
  }
}
