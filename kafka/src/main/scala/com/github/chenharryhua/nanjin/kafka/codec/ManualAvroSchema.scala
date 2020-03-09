package com.github.chenharryhua.nanjin.kafka.codec

import cats.implicits._
import com.sksamuel.avro4s.{
  DefaultFieldMapper,
  SchemaFor,
  Decoder => AvroDecoder,
  Encoder => AvroEncoder
}
import org.apache.avro.SchemaCompatibility.SchemaCompatibilityType
import org.apache.avro.{Schema, SchemaCompatibility}

import scala.language.experimental.macros

final class ManualAvroSchema[A](val schemaText: String)(
  implicit
  val schemaFor: SchemaFor[A],
  val avroDecoder: AvroDecoder[A],
  val avroEncoder: AvroEncoder[A]) {
  val schema: Schema = (new Schema.Parser).parse(schemaText)
}

object ManualAvroSchema {

  @throws[Exception]
  def unsafeFrom[A: AvroDecoder: AvroEncoder: SchemaFor](
    schemaText: String): ManualAvroSchema[A] = {
    val inferred = SchemaFor[A].schema(DefaultFieldMapper)
    val input    = (new Schema.Parser).parse(schemaText)

    val rw = SchemaCompatibility.checkReaderWriterCompatibility(inferred, input).getType
    val wr = SchemaCompatibility.checkReaderWriterCompatibility(input, inferred).getType

    require(SchemaCompatibility.schemaNameEquals(inferred, input), "schema name is different")
    require(SchemaCompatibilityType.COMPATIBLE.compareTo(rw) === 0, "incompatible schema - rw")
    require(SchemaCompatibilityType.COMPATIBLE.compareTo(wr) === 0, "incompatible schema - wr")
    new ManualAvroSchema[A](schemaText)
  }

  def apply[A](schemaText: String)(
    implicit
    schemaFor: SchemaFor[A],
    avroDecoder: AvroDecoder[A],
    avroEncoder: AvroEncoder[A]): ManualAvroSchema[A] =
    macro ManualAvroMacro.impl[A]
}
