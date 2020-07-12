package com.github.chenharryhua.nanjin.messages.kafka.codec

import cats.implicits._
import com.sksamuel.avro4s.{SchemaFor, Decoder => AvroDecoder, Encoder => AvroEncoder}
import org.apache.avro.SchemaCompatibility.SchemaCompatibilityType
import org.apache.avro.{Schema, SchemaCompatibility}

final case class WithAvroSchema[A] private (
  schemaFor: SchemaFor[A],
  avroDecoder: AvroDecoder[A],
  avroEncoder: AvroEncoder[A])

object WithAvroSchema {

  def apply[A: AvroDecoder: AvroEncoder](input: Schema): WithAvroSchema[A] = {
    val inferred: Schema = AvroEncoder[A].schema

    val rw = SchemaCompatibility.checkReaderWriterCompatibility(inferred, input).getType
    val wr = SchemaCompatibility.checkReaderWriterCompatibility(input, inferred).getType

    require(SchemaCompatibility.schemaNameEquals(inferred, input), "schema name is different")

    if (SchemaCompatibilityType.COMPATIBLE.compareTo(rw) =!= 0)
      println(s"catch attention - rw:\ninput:\n$input\ninfered:\n$inferred")

    if (SchemaCompatibilityType.COMPATIBLE.compareTo(wr) =!= 0)
      println(s"catch attention - wr:\ninput:\n$input\ninfered:\n$inferred")

    val sf: SchemaFor[A] = SchemaFor[A](input)
    WithAvroSchema(sf, AvroDecoder[A].withSchema(sf), AvroEncoder[A].withSchema(sf))
  }

  def apply[A: AvroDecoder: AvroEncoder](schemaText: String): WithAvroSchema[A] = {
    val schema = (new Schema.Parser).parse(schemaText)
    apply[A](schema)
  }
}
