package com.github.chenharryhua.nanjin.messages.kafka.codec

import cats.implicits._
import com.sksamuel.avro4s.{Decoder => AvroDecoder, Encoder => AvroEncoder}
import org.apache.avro.SchemaCompatibility.SchemaCompatibilityType
import org.apache.avro.{Schema, SchemaCompatibility}

final case class WithAvroSchema[A] private (
  schema: Schema,
  avroDecoder: AvroDecoder[A],
  avroEncoder: AvroEncoder[A])

object WithAvroSchema {

  @throws[Exception]
  def apply[A: AvroDecoder: AvroEncoder](schemaText: String): WithAvroSchema[A] = {
    require(AvroDecoder[A].schema == AvroEncoder[A].schema)

    val inferred: Schema = AvroEncoder[A].schema
    val input: Schema    = (new Schema.Parser).parse(schemaText)

    val rw = SchemaCompatibility.checkReaderWriterCompatibility(inferred, input).getType
    val wr = SchemaCompatibility.checkReaderWriterCompatibility(input, inferred).getType

    require(SchemaCompatibility.schemaNameEquals(inferred, input), "schema name is different")

    if (SchemaCompatibilityType.COMPATIBLE.compareTo(rw) =!= 0)
      println(s"catch attention - rw:\ninput:\n$input\ninfered:\n$inferred")

    if (SchemaCompatibilityType.COMPATIBLE.compareTo(wr) =!= 0)
      println(s"catch attention - wr:\ninput:\n$input\ninfered:\n$inferred")

    new WithAvroSchema[A](input, AvroDecoder[A], AvroEncoder[A])
  }
}
