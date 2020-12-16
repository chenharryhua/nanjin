package com.github.chenharryhua.nanjin.messages.kafka.codec

import cats.data.Ior
import cats.syntax.all._
import com.sksamuel.avro4s.{
  DecoderHelpers,
  EncoderHelpers,
  SchemaFor,
  Decoder => AvroDecoder,
  Encoder => AvroEncoder
}
import eu.timepit.refined.string.MatchesRegex
import eu.timepit.refined.{refineV, W}
import io.circe.optics.JsonPath
import io.circe.optics.JsonPath._
import io.circe.{parser, Json}
import org.apache.avro.SchemaCompatibility.SchemaCompatibilityType
import org.apache.avro.{Schema, SchemaCompatibility}

import scala.util.Try

final case class AvroCodec[A](
  schemaFor: SchemaFor[A],
  avroDecoder: AvroDecoder[A],
  avroEncoder: AvroEncoder[A]) {
  val schema: Schema        = schemaFor.schema
  def idConversion(a: A): A = avroDecoder.decode(avroEncoder.encode(a))

  /** https://avro.apache.org/docs/current/spec.html
    * the grammar for a namespace is:
    *
    * <empty> | <name>[(<dot><name>)*]
    *
    * empty namespace is not allowed
    */

  def withNamespace(namespace: String): AvroCodec[A] = {
    type Namespace = MatchesRegex[W.`"^[a-zA-Z0-9_.]+$"`.T]
    val res = for {
      ns <- refineV[Namespace](namespace)
      json <- parser
        .parse(schema.toString)
        .map(x => root.namespace.string.set(ns.value)(x))
        .map(_.noSpaces)
        .leftMap(_.getMessage())
      ac <- AvroCodec.build[A](AvroCodec.toSchemaFor[A](json), avroDecoder, avroEncoder)
    } yield ac
    res match {
      case Left(ex)  => sys.error(s"$ex, input namespace: $namespace")
      case Right(ac) => ac
    }
  }

  def at(jsonPath: JsonPath): Either[String, Json] = for {
    json <- parser.parse(schema.toString()).leftMap(_.message)
    jsonObject <- jsonPath.obj.getOption(json).toRight("unable to find child schema")
  } yield Json.fromJsonObject(jsonObject)

  /** @param jsonPath path to the child schema
    * @return
    */
  def child[B](
    jsonPath: JsonPath)(implicit dec: AvroDecoder[B], enc: AvroEncoder[B]): AvroCodec[B] = {
    val oa = for {
      json <- at(jsonPath)
      ac <- AvroCodec.build[B](AvroCodec.toSchemaFor[B](json.noSpaces), dec, enc)
    } yield ac
    oa match {
      case Left(ex)  => sys.error(ex)
      case Right(ac) => ac
    }
  }
}

/** left  - error
  * right - AvroCodec
  * both  - (warnings, AvroCodec)
  */
object AvroCodec {

  def toSchema(jsonStr: String): Schema =
    (new Schema.Parser).parse(jsonStr)

  def toSchemaFor[A](jsonStr: String): SchemaFor[A] =
    SchemaFor[A](toSchema(jsonStr))

  def build[A](
    schemaFor: SchemaFor[A],
    decoder: AvroDecoder[A],
    encoder: AvroEncoder[A]): Either[String, AvroCodec[A]] =
    for {
      d <-
        Either
          .catchNonFatal(DecoderHelpers.buildWithSchema(decoder, schemaFor))
          .leftMap(_ => "avro4s decline decode schema change")
      e <-
        Either
          .catchNonFatal(EncoderHelpers.buildWithSchema(encoder, schemaFor))
          .leftMap(_ => "avro4s decline encode schema change")
    } yield AvroCodec(schemaFor, d, e)

  def apply[A](input: Schema)(implicit
    decoder: AvroDecoder[A],
    encoder: AvroEncoder[A]): Ior[String, AvroCodec[A]] = {
    val inferred: Schema = encoder.schema

    if (SchemaCompatibility.schemaNameEquals(inferred, input)) {
      val rw = SchemaCompatibility.checkReaderWriterCompatibility(inferred, input).getType
      val wr = SchemaCompatibility.checkReaderWriterCompatibility(input, inferred).getType

      val rwCompat: Option[String] =
        if (SchemaCompatibilityType.COMPATIBLE.compareTo(rw) =!= 0)
          Some("read-write incompatiable.")
        else None

      val wrCompat: Option[String] =
        if (SchemaCompatibilityType.COMPATIBLE.compareTo(wr) =!= 0)
          Some("write-read incompatiable.")
        else None

      val compat: Option[String]            = rwCompat |+| wrCompat
      val esa: Either[String, AvroCodec[A]] = build(SchemaFor[A](input), decoder, encoder)
      Ior.fromEither(esa).flatMap { w =>
        compat.fold[Ior[String, AvroCodec[A]]](Ior.right(w))(warn => Ior.both(warn, w))
      }
    } else
      Ior.left("schema name is different")
  }

  def apply[A: AvroDecoder: AvroEncoder](schemaText: String): Ior[String, AvroCodec[A]] = {
    val codec: Either[String, Ior[String, AvroCodec[A]]] =
      Try(toSchema(schemaText)).flatMap(s => Try(apply(s))).toEither.leftMap(_.getMessage)
    Ior.fromEither(codec).flatten
  }

  def unsafe[A: AvroDecoder: AvroEncoder](schemaText: String): AvroCodec[A] =
    apply[A](schemaText).toEither match {
      case Right(r) => r
      case Left(ex) => sys.error(ex)
    }

  def apply[A: AvroDecoder: AvroEncoder: SchemaFor]: AvroCodec[A] =
    AvroCodec(SchemaFor[A], AvroDecoder[A], AvroEncoder[A])
}
