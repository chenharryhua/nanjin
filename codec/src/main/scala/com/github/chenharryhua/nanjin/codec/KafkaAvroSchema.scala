package com.github.chenharryhua.nanjin.codec

import java.io.File

import cats.implicits._
import com.sksamuel.avro4s.{AvroSchema, Decoder => AvroDecoder, Encoder => AvroEncoder, SchemaFor}
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

abstract class KafkaAvroSchema[A: SchemaFor](val schema: Schema)(
  implicit
  val decoder: AvroDecoder[A],
  val encoder: AvroEncoder[A]) {
  implicit val lcs: Patience[Json] = new Patience[Json]

  private val inferedSchema: Schema = AvroSchema[A]

  private def cleanupJsonDocument: Json => Json = {
    val noVersion   = root.at("version").set(None)
    val noDoc       = root.at("doc").set(None).andThen(root.fields.each.at("doc").set(None))
    val noJavaClass = root.fields.each.at("java-class").set(None)
    noVersion.andThen(noDoc).andThen(noJavaClass)
  }

  def isSame: Either[ParsingFailure, JsonPatch[Json]] =
    (parse(schema.toString()), parse(inferedSchema.toString)).mapN { (f, s) =>
      diff(cleanupJsonDocument(f), cleanupJsonDocument(s))
    }

  def isCompatiable: Boolean =
    SchemaCompatibility
      .checkReaderWriterCompatibility(schema, inferedSchema)
      .getResult
      .getCompatibility == SchemaCompatibilityType.COMPATIBLE &&
      SchemaCompatibility
        .checkReaderWriterCompatibility(inferedSchema, schema)
        .getResult
        .getCompatibility == SchemaCompatibilityType.COMPATIBLE

}

object KafkaAvroSchema {
  val parser: Schema.Parser = new Schema.Parser

  def apply[A](implicit ev: KafkaAvroSchema[A]): KafkaAvroSchema[A] = ev

  def apply[A: AvroDecoder: AvroEncoder: SchemaFor](str: String): KafkaAvroSchema[A] =
    new KafkaAvroSchema[A](parser.parse(str)) {}

  def apply[A: AvroDecoder: AvroEncoder: SchemaFor](file: File): KafkaAvroSchema[A] =
    new KafkaAvroSchema[A](parser.parse(file)) {}
}
