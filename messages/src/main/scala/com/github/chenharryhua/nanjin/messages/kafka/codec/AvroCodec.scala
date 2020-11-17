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
import io.circe.optics.JsonPath._
import io.circe.parser
import org.apache.avro.SchemaCompatibility.SchemaCompatibilityType
import org.apache.avro.{Schema, SchemaCompatibility}

import scala.util.Try

final case class AvroCodec[A](
  schemaFor: SchemaFor[A],
  avroDecoder: AvroDecoder[A],
  avroEncoder: AvroEncoder[A]) {
  val schema: Schema        = schemaFor.schema
  def idConversion(a: A): A = avroDecoder.decode(avroEncoder.encode(a))

  def withNamespace(ns: String): AvroCodec[A] = {
    val json = parser
      .parse(schema.toString)
      .map(x => root.namespace.string.modify(_ => ns)(x))
      .map(_.noSpaces) match {
      case Left(ex)     => throw ex
      case Right(value) => value
    }
    val newSchema: Schema = (new Schema.Parser).parse(json)
    val sf: SchemaFor[A]  = SchemaFor[A](newSchema)
    AvroCodec[A](sf, avroDecoder.withSchema(sf), avroEncoder.withSchema(sf))
  }
}

/** left  - error
  * right - WithAvroSchema
  * both  - (warnings, WithAvroSchema)
  */
object AvroCodec {

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

      val compat: Option[String]  = rwCompat |+| wrCompat
      val schemaFor: SchemaFor[A] = SchemaFor[A](input)
      val esa: Either[String, AvroCodec[A]] = for {
        d <-
          Either
            .catchNonFatal(DecoderHelpers.buildWithSchema(decoder, schemaFor))
            .leftMap(_ => "avro4s decline decode schema change")
        e <-
          Either
            .catchNonFatal(EncoderHelpers.buildWithSchema(encoder, schemaFor))
            .leftMap(_ => "avro4s decline encode schema change")
      } yield AvroCodec(schemaFor, d, e)
      Ior.fromEither(esa).flatMap { w =>
        compat.fold[Ior[String, AvroCodec[A]]](Ior.right(w))(warn => Ior.both(warn, w))
      }
    } else
      Ior.left("schema name is different")
  }

  def apply[A: AvroDecoder: AvroEncoder](schemaText: String): Ior[String, AvroCodec[A]] = {
    val codec: Either[String, Ior[String, AvroCodec[A]]] =
      Try((new Schema.Parser).parse(schemaText))
        .flatMap(s => Try(apply(s)))
        .toEither
        .leftMap(_.getMessage)
    Ior.fromEither(codec).flatten
  }

  def apply[A: AvroDecoder: AvroEncoder: SchemaFor]: AvroCodec[A] =
    AvroCodec(SchemaFor[A], AvroDecoder[A], AvroEncoder[A])
}
