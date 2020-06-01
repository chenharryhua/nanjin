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

    if (SchemaCompatibilityType.COMPATIBLE.compareTo(rw) =!= 0)
      println(s"incompatible schema - rw:\ninput:\n${input}\ninfered:\n${inferred}")

    if (SchemaCompatibilityType.COMPATIBLE.compareTo(wr) =!= 0)
      println(s"incompatible schema - wr:\ninput:\n${input}\ninfered:\n$inferred")

    new ManualAvroSchema[A](
      AvroDecoder[A].withSchema(SchemaFor[A](input)),
      AvroEncoder[A].withSchema(SchemaFor[A](input)))
  }
}
