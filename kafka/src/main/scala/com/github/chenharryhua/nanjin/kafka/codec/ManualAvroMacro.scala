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

final class ManualAvroMacro(val c: blackbox.Context) extends MacroUtils {
  import c.universe._

  def impl[A: c.WeakTypeTag](schemaText: c.Expr[String])(
    schemaFor: c.Expr[SchemaFor[A]],
    avroDecoder: c.Expr[AvroDecoder[A]],
    avroEncoder: c.Expr[AvroEncoder[A]]): c.Expr[ManualAvroSchema[A]] = {
    val sf: Schema = eval(schemaFor).schema(DefaultFieldMapper)
    val sk: Schema = (new Schema.Parser).parse(eval(schemaText))

    println(s"infered schema:\n ${sf.toString(true)}")

    val rw: SchemaCompatibilityType =
      SchemaCompatibility.checkReaderWriterCompatibility(sf, sk).getType
    val wr: SchemaCompatibilityType =
      SchemaCompatibility.checkReaderWriterCompatibility(sk, sf).getType

    if (!SchemaCompatibility.schemaNameEquals(sk, sf))
      abort("schema name is different")
    else if (SchemaCompatibilityType.COMPATIBLE.compareTo(rw) != 0)
      abort("incompatible schema - rw")
    else if (SchemaCompatibilityType.COMPATIBLE.compareTo(wr) != 0)
      abort("incompatible schema - wr")
    else
      c.Expr[ManualAvroSchema[A]](
        q""" new _root_.com.github.chenharryhua.nanjin.kafka.codec.ManualAvroSchema($schemaText)($schemaFor,$avroDecoder,$avroEncoder) """)
  }
}
