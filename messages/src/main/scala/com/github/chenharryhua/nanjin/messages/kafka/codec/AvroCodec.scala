package com.github.chenharryhua.nanjin.messages.kafka.codec

import com.sksamuel.avro4s.{Decoder as AvroDecoder, Encoder as AvroEncoder, Record, SchemaFor}
import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord

final class AvroCodec[A] private (
  val schemaFor: SchemaFor[A],
  val avroDecoder: AvroDecoder[A],
  val avroEncoder: AvroEncoder[A]) {
    
  val schema:Schema = schemaFor.schema

  private val decoder: Any => A = avroDecoder.decode(schema)
  private val encoder: A => AnyRef = avroEncoder.encode(schema)

  def idConversion(a: A): A = decoder(encoder(a))

  def withSchema(schema: Schema): AvroCodec[A] =
    AvroCodec(schema)(avroDecoder, avroEncoder, schemaFor)

  def toRecord(value: A): Record = encoder(value) match {
    case record: Record => record
    case other          => sys.error(s"${other.getClass.getName} is not com.sksamuel.avro4s.Record")
  }
  def fromRecord(value: IndexedRecord): A = decoder(value)

  /** https://avro.apache.org/docs/current/spec.html the grammar for a namespace is:
    *
    * <empty> | <name>[(<dot><name>)*]
    *
    * empty namespace is not allowed
    */
  def withNamespace(namespace: String): AvroCodec[A] =
    withSchema(replaceNamespace(schemaFor.schema, namespace))

  def withoutNamespace: AvroCodec[A] = withSchema(removeNamespace(schemaFor.schema))
  def withoutDefaultField: AvroCodec[A] = withSchema(removeDefaultField(schemaFor.schema))
  def withoutDoc: AvroCodec[A] = withSchema(removeDocField(schemaFor.schema))
}

object AvroCodec {
  def apply[A](sf: SchemaFor[A], dc: AvroDecoder[A], ec: AvroEncoder[A]): AvroCodec[A] =
    new AvroCodec[A](sf, dc, ec)

  def apply[A](implicit dc: AvroDecoder[A], ec: AvroEncoder[A], sf: SchemaFor[A]): AvroCodec[A] =
    apply[A](sf, dc, ec)

  def apply[A](
    schema: Schema)(implicit dc: AvroDecoder[A], ec: AvroEncoder[A], sf: SchemaFor[A]): AvroCodec[A] = {
    val b = backwardCompatibility(sf.schema, schema)
    val f = forwardCompatibility(sf.schema, schema)
    if (b.isEmpty && f.isEmpty) {
      apply(SchemaFor[A](schema), dc, ec)
    } else {
      val msg = (b ++ f).map(m => s"${m.getMessage} at ${m.getLocation}").mkString("\n")
      throw new Exception(s"incompatible schema: $msg") // scalafix:ok
    }
  }

  def apply[A: AvroDecoder: AvroEncoder: SchemaFor](schemaText: String): AvroCodec[A] =
    apply[A]((new Schema.Parser).parse(schemaText))
}
