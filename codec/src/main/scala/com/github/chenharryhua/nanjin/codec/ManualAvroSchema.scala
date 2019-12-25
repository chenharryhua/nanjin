package com.github.chenharryhua.nanjin.codec

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
import org.apache.avro.Schema

final case class KafkaAvroSchemaError(msg: String) extends Exception(msg)

final case class ManualAvroSchema[A] private (schema: Schema)(
  implicit
  val decoder: AvroDecoder[A],
  val encoder: AvroEncoder[A])

object ManualAvroSchema {

  implicit val lcs: Patience[Json] = new Patience[Json]

  private def cleanupJsonDocument: Json => Json = {
    val noVersion   = root.at("version").set(None)
    val noDoc       = root.at("doc").set(None).andThen(root.fields.each.at("doc").set(None))
    val noJavaClass = root.fields.each.at("java-class").set(None)
    noVersion.andThen(noDoc).andThen(noJavaClass)
  }

  private def whatsDifferent(
    inputSchema: String,
    inferredSchema: Schema): Either[KafkaAvroSchemaError, JsonPatch[Json]] =
    (parse(inputSchema), parse(inferredSchema.toString)).mapN { (input, inferred) =>
      diff(cleanupJsonDocument(input), cleanupJsonDocument(inferred))
    }.leftMap(e => KafkaAvroSchemaError(e.message))

  @throws[KafkaAvroSchemaError]
  def apply[A: AvroDecoder: AvroEncoder: SchemaFor](stringSchema: String): ManualAvroSchema[A] =
    whatsDifferent(stringSchema, AvroSchema[A]).flatMap { jp =>
      if (jp.ops.isEmpty) {
        val parser: Schema.Parser = new Schema.Parser
        Right(ManualAvroSchema(parser.parse(stringSchema)))
      } else Left(KafkaAvroSchemaError(jp.ops.map(_.toString).mkString("\n")))
    }.fold(throw _, identity)
}
