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

final class ManualAvroSchema[A] private (val schema: Schema)(
  implicit
  val avroDecoder: AvroDecoder[A],
  val avroEncoder: AvroEncoder[A],
  val schemaFor: SchemaFor[A])

object ManualAvroSchema {

  @throws[Exception]
  def apply[A: AvroDecoder: AvroEncoder: SchemaFor](stringSchema: String): ManualAvroSchema[A] = {
    val inferred = SchemaFor[A].schema(DefaultFieldMapper)
    val input    = (new Schema.Parser).parse(stringSchema)

    val rw = SchemaCompatibility.checkReaderWriterCompatibility(inferred, input).getType
    val wr = SchemaCompatibility.checkReaderWriterCompatibility(input, inferred).getType
    
    require(SchemaCompatibility.schemaNameEquals(inferred, input), "schema name is different")
    require(SchemaCompatibilityType.COMPATIBLE.compareTo(rw) === 0, "incompatible schema - rw")
    require(SchemaCompatibilityType.COMPATIBLE.compareTo(wr) === 0, "incompatible schema - wr")
    new ManualAvroSchema[A](input)
  }
}
