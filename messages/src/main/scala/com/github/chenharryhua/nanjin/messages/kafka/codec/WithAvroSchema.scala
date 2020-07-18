package com.github.chenharryhua.nanjin.messages.kafka.codec

import cats.data.Ior
import cats.implicits._
import com.sksamuel.avro4s.{SchemaFor, Decoder => AvroDecoder, Encoder => AvroEncoder}
import org.apache.avro.SchemaCompatibility.SchemaCompatibilityType
import org.apache.avro.{Schema, SchemaCompatibility}

import scala.util.Try

final case class WithAvroSchema[A] private (
  schemaFor: SchemaFor[A],
  avroDecoder: AvroDecoder[A],
  avroEncoder: AvroEncoder[A])

/**
  * left  - error
  * right - WithAvroSchema
  * both  - (warnings, WithAvroSchema)
  */
object WithAvroSchema {

  def apply[A](input: Schema)(implicit
    decoder: AvroDecoder[A],
    encoder: AvroEncoder[A]): Ior[String, WithAvroSchema[A]] = {
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

      val compat: Option[String] = rwCompat |+| wrCompat
      val sf: SchemaFor[A]       = SchemaFor[A](input)
      val was: Either[String, WithAvroSchema[A]] = for {
        d <-
          Either
            .catchNonFatal(decoder.withSchema(sf))
            .leftMap(_ => "avro4s decline decode schema change")
        e <-
          Either
            .catchNonFatal(encoder.withSchema(sf))
            .leftMap(_ => "avro4s decline encode schema change")
      } yield WithAvroSchema(sf, d, e)
      Ior.fromEither(was).flatMap { w =>
        compat.fold[Ior[String, WithAvroSchema[A]]](Ior.right(w))(warn => Ior.both(warn, w))
      }
    } else
      Ior.left("schema name is different")
  }

  def apply[A: AvroDecoder: AvroEncoder](schemaText: String): Ior[String, WithAvroSchema[A]] =
    Try((new Schema.Parser).parse(schemaText)).fold[Ior[String, WithAvroSchema[A]]](
      ex => Ior.left(ex.getMessage),
      schema => apply(schema)
    )
}
