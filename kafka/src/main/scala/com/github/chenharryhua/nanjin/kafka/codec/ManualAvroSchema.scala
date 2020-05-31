package com.github.chenharryhua.nanjin.kafka.codec

import cats.implicits._
import com.sksamuel.avro4s.{SchemaFor, Decoder => AvroDecoder, Encoder => AvroEncoder}
import org.apache.avro.SchemaCompatibility.SchemaCompatibilityType
import org.apache.avro.{Schema, SchemaCompatibility}

import scala.language.experimental.macros

final case class ManualAvroSchema[A] private (
  avroDecoder: AvroDecoder[A],
  avroEncoder: AvroEncoder[A])

object ManualAvroSchema {

  @throws[Exception]
  def unsafeFrom[A: AvroDecoder: AvroEncoder](schemaText: String): ManualAvroSchema[A] = {
    require(AvroDecoder[A].schema == AvroEncoder[A].schema)

    val inferred = AvroEncoder[A].schema
    val input    = (new Schema.Parser).parse(schemaText)

    val rw = SchemaCompatibility.checkReaderWriterCompatibility(inferred, input).getType
    val wr = SchemaCompatibility.checkReaderWriterCompatibility(input, inferred).getType

    require(SchemaCompatibility.schemaNameEquals(inferred, input), "schema name is different")
    require(SchemaCompatibilityType.COMPATIBLE.compareTo(rw) === 0, "incompatible schema - rw")
    require(SchemaCompatibilityType.COMPATIBLE.compareTo(wr) === 0, "incompatible schema - wr")
    new ManualAvroSchema[A](
      AvroDecoder[A].withSchema(SchemaFor(input)),
      AvroEncoder[A].withSchema(SchemaFor(input)))
  }

  /*
  @SuppressWarnings(Array("all"))
  def apply[A](schemaText: String)(implicit
    schemaFor: SchemaFor[A],
    avroDecoder: AvroDecoder[A],
    avroEncoder: AvroEncoder[A]): ManualAvroSchema[A] =
    macro ManualAvroMacro.impl[A]
   */
}
