package com.github.chenharryhua.nanjin.kafka

import cats.implicits._
import com.sksamuel.avro4s.{AvroSchema, SchemaFor, Decoder => AvroDecoder, Encoder => AvroEncoder}
import diffson._
import diffson.circe._
import diffson.jsonpatch.lcsdiff._
import diffson.lcs.Patience
import io.circe.Json
import io.circe.optics.JsonOptics._
import io.circe.optics.JsonPath._
import io.circe.parser._
import monocle.function.Plated
import org.apache.avro.Schema

final case class KafkaAvroSchemaException(msg: String) extends Exception(msg)

final class ManualAvroSchema[A] private (val schema: Schema)(
  implicit
  val decoder: AvroDecoder[A],
  val encoder: AvroEncoder[A],
  val schemaFor: SchemaFor[A])

object ManualAvroSchema {

  implicit val lcs: Patience[Json] = new Patience[Json]

  private def cleanupJsonDocument: Json => Json = {
    val noVersion = root.at("version").set(None)
    val noDoc: Json => Json =
      root.at("doc").set(None).andThen(Plated.transform[Json](j => root.at("doc").set(None)(j)))
    val noJavaClass: Json => Json =
      Plated.transform[Json](j => root.at("java-class").set(None)(j))

    noVersion.andThen(noDoc).andThen(noJavaClass)
  }

  private def whatsDifferent(
    inputSchema: String,
    inferredSchema: Schema): Either[KafkaAvroSchemaException, Schema] =
    (parse(inputSchema), parse(inferredSchema.toString)).mapN { (input, inferred) =>
      val jp = diff(cleanupJsonDocument(input), cleanupJsonDocument(inferred))
      if (jp.ops.isEmpty) {
        val parser: Schema.Parser = new Schema.Parser
        Right[KafkaAvroSchemaException, Schema](parser.parse(inputSchema))
      } else
        Left(KafkaAvroSchemaException(s"""
                                         |Input Schema is different than inferred schema
                                         |errrors: 
                                         |${jp.ops.map(_.toString).mkString("\n")}
                                         |input schema: (after cleanup)
                                         |$input
                                         |inferred schema: (after cleanup)
                                         |$inferred""".stripMargin))

    }.leftMap(e => KafkaAvroSchemaException(e.message)).flatten

  @throws[KafkaAvroSchemaException]
  def apply[A: AvroDecoder: AvroEncoder: SchemaFor](stringSchema: String): ManualAvroSchema[A] =
    whatsDifferent(stringSchema, AvroSchema[A])
      .map(new ManualAvroSchema[A](_))
      .fold(throw _, identity)
}
