package com.github.chenharryhua.nanjin.kafka.codec

import com.sksamuel.avro4s.{
  DefaultFieldMapper,
  SchemaFor,
  Decoder => AvroDecoder,
  Encoder => AvroEncoder
}
import eu.timepit.refined.macros.MacroUtils
import org.apache.avro.SchemaCompatibility.SchemaCompatibilityType
import org.apache.avro.{Schema, SchemaCompatibility}

import scala.reflect.macros.blackbox

@SuppressWarnings(Array("all"))
final class ManualAvroMacro(val c: blackbox.Context) extends MacroUtils {
  import c.universe._

  def impl[A: c.WeakTypeTag](schemaText: c.Expr[String])(
    schemaFor: c.Expr[SchemaFor[A]],
    avroDecoder: c.Expr[AvroDecoder[A]],
    avroEncoder: c.Expr[AvroEncoder[A]]): c.Expr[ManualAvroSchema[A]] = {
    val sf: Schema = eval(schemaFor).schema
    val st: Schema = (new Schema.Parser).parse(eval(schemaText))

    val rw: SchemaCompatibilityType =
      SchemaCompatibility.checkReaderWriterCompatibility(sf, st).getType
    val wr: SchemaCompatibilityType =
      SchemaCompatibility.checkReaderWriterCompatibility(st, sf).getType

    val inferred: String = sf.toString(true)

    if (!SchemaCompatibility.schemaNameEquals(st, sf))
      abort(s"schema name is different - $inferred")
    else if (SchemaCompatibilityType.COMPATIBLE.compareTo(rw) != 0)
      abort(s"incompatible schema - $inferred")
    else if (SchemaCompatibilityType.COMPATIBLE.compareTo(wr) != 0)
      abort(s"incompatible schema - $inferred")
    else
      c.Expr[ManualAvroSchema[A]](
        q""" new _root_.com.github.chenharryhua.nanjin.kafka.codec.ManualAvroSchema($schemaText)($schemaFor,$avroDecoder,$avroEncoder) """)
  }
}
