package com.github.chenharryhua.nanjin.messages.kafka.codec

import cats.data.Ior
import cats.syntax.all.*
import com.sksamuel.avro4s.{
  Decoder as AvroDecoder,
  DecoderHelpers,
  Encoder as AvroEncoder,
  EncoderHelpers,
  FromRecord,
  Record,
  SchemaFor,
  ToRecord
}
import eu.timepit.refined.refineV
import eu.timepit.refined.string.MatchesRegex
import io.circe.optics.JsonPath
import io.circe.optics.JsonPath.*
import io.circe.{parser, Json}
import org.apache.avro.SchemaCompatibility.SchemaCompatibilityType
import org.apache.avro.generic.IndexedRecord
import org.apache.avro.{Schema, SchemaCompatibility, SchemaParseException}

import scala.util.Try

final case class NJAvroCodec[A](schemaFor: SchemaFor[A], avroDecoder: AvroDecoder[A], avroEncoder: AvroEncoder[A]) {
  val schema: Schema        = schemaFor.schema
  def idConversion(a: A): A = avroDecoder.decode(avroEncoder.encode(a))

  private[this] val toRec: ToRecord[A]     = ToRecord(avroEncoder)
  private[this] val fromRec: FromRecord[A] = FromRecord(avroDecoder)

  def toRecord(a: A): Record           = toRec.to(a)
  def fromRecord(ir: IndexedRecord): A = fromRec.from(ir)

  /** https://avro.apache.org/docs/current/spec.html the grammar for a namespace is:
    *
    * <empty> | <name>[(<dot><name>)*]
    *
    * empty namespace is not allowed
    */
  @throws[Exception]
  def withNamespace(namespace: String): NJAvroCodec[A] = {
    type Namespace = MatchesRegex["^[a-zA-Z0-9_.]+$"]
    val res = for {
      ns <- refineV[Namespace](namespace)
      json <- parser
        .parse(schema.toString)
        .map(x => root.namespace.string.set(ns.value)(x))
        .map(_.noSpaces)
        .leftMap(_.getMessage())
      ac <- NJAvroCodec.build[A](NJAvroCodec.toSchemaFor[A](json), avroDecoder, avroEncoder)
    } yield ac
    res match {
      case Left(ex)  => sys.error(s"$ex, input namespace: $namespace")
      case Right(ac) => ac
    }
  }

  def withoutNamespace: NJAvroCodec[A] = {
    val res = for {
      json <- parser
        .parse(schema.toString)
        .toOption
        .flatMap(_.hcursor.downField("namespace").delete.top.map(_.noSpaces))
      ac <- NJAvroCodec.build[A](NJAvroCodec.toSchemaFor[A](json), avroDecoder, avroEncoder).toOption
    } yield ac
    res.getOrElse(this)
  }

  def at(jsonPath: JsonPath): Either[String, Json] = for {
    json <- parser.parse(schema.toString()).leftMap(_.message)
    jsonObject <- jsonPath.obj.getOption(json).toRight("unable to find child schema")
  } yield Json.fromJsonObject(jsonObject)

  /** @param jsonPath
    *   path to the child schema
    * @return
    */
  @throws[Exception]
  def child[B](jsonPath: JsonPath)(implicit dec: AvroDecoder[B], enc: AvroEncoder[B]): NJAvroCodec[B] = {
    val oa = for {
      json <- at(jsonPath)
      ac <- NJAvroCodec.build[B](NJAvroCodec.toSchemaFor[B](json.noSpaces), dec, enc)
    } yield ac
    oa match {
      case Left(ex)  => sys.error(ex)
      case Right(ac) => ac
    }
  }
}

/** left - error right - AvroCodec both - (warnings, AvroCodec)
  */
object NJAvroCodec {

  private def build[A](
    schemaFor: SchemaFor[A],
    decoder: AvroDecoder[A],
    encoder: AvroEncoder[A]): Either[String, NJAvroCodec[A]] =
    for {
      d <-
        Either
          .catchNonFatal(DecoderHelpers.buildWithSchema(decoder, schemaFor))
          .leftMap(_ => "avro4s decline decode schema change")
      e <-
        Either
          .catchNonFatal(EncoderHelpers.buildWithSchema(encoder, schemaFor))
          .leftMap(_ => "avro4s decline encode schema change")
    } yield NJAvroCodec(schemaFor, d, e)

  @throws[SchemaParseException]
  private def toSchemaFor[A](jsonStr: String): SchemaFor[A] = SchemaFor[A](toSchema(jsonStr))

  @throws[SchemaParseException]
  def toSchema(jsonStr: String): Schema = (new Schema.Parser).parse(jsonStr)

  def apply[A](
    input: Schema)(implicit decoder: AvroDecoder[A], encoder: AvroEncoder[A]): Ior[String, NJAvroCodec[A]] = {
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

      val compat: Option[String]              = rwCompat |+| wrCompat
      val esa: Either[String, NJAvroCodec[A]] = build(SchemaFor[A](input), decoder, encoder)
      Ior.fromEither(esa).flatMap { w =>
        compat.fold[Ior[String, NJAvroCodec[A]]](Ior.right(w))(warn => Ior.both(warn, w))
      }
    } else
      Ior.left("schema name is different")
  }

  def apply[A: AvroDecoder: AvroEncoder](schemaText: String): Ior[String, NJAvroCodec[A]] = {
    val codec: Either[String, Ior[String, NJAvroCodec[A]]] =
      Try(toSchema(schemaText)).flatMap(s => Try(apply(s))).toEither.leftMap(_.getMessage)
    Ior.fromEither(codec).flatten
  }

  @throws[Exception]
  def unsafe[A: AvroDecoder: AvroEncoder](schemaText: String): NJAvroCodec[A] =
    apply[A](schemaText).toEither match {
      case Right(r) => r
      case Left(ex) => sys.error(ex)
    }

  def apply[A: AvroDecoder: AvroEncoder: SchemaFor]: NJAvroCodec[A] =
    NJAvroCodec(SchemaFor[A], AvroDecoder[A], AvroEncoder[A])
}
