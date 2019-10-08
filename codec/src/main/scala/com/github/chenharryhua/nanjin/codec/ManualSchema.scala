package com.github.chenharryhua.nanjin.codec

import cats.tagless.finalAlg
import com.sksamuel.avro4s.{AvroSchema, Decoder, Encoder, SchemaFor}
import org.apache.avro.SchemaCompatibility.SchemaCompatibilityType
import org.apache.avro.{Schema, SchemaCompatibility}

@finalAlg
abstract class ManualSchema[A: SchemaFor](
  implicit val decoder: Decoder[A],
  val encoder: Encoder[A]) {
  def schema: Schema

  final def isCompatiable: Boolean =
    SchemaCompatibility
      .checkReaderWriterCompatibility(schema, AvroSchema[A])
      .getResult
      .getCompatibility == SchemaCompatibilityType.COMPATIBLE &&
      SchemaCompatibility
        .checkReaderWriterCompatibility(AvroSchema[A], schema)
        .getResult
        .getCompatibility == SchemaCompatibilityType.COMPATIBLE

  require(isCompatiable)
}
