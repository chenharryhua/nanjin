package com.github.chenharryhua.nanjin.messages.kafka.codec

import com.sksamuel.avro4s.{
  Codec,
  Decoder as AvroDecoder,
  DecoderHelpers,
  Encoder as AvroEncoder,
  EncoderHelpers,
  Record,
  SchemaFor
}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.string.MatchesRegex
import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord

import scala.util.Try

final class AvroCodec[A] private (
  override val schemaFor: SchemaFor[A],
  avroDecoder: AvroDecoder[A],
  avroEncoder: AvroEncoder[A])
    extends Codec[A] {

  def idConversion(a: A): A = avroDecoder.decode(avroEncoder.encode(a))

  def withSchema(schema: Schema): AvroCodec[A] =
    AvroCodec(schema)(avroDecoder, avroEncoder, schemaFor)

  override def withSchema(schemaFor: SchemaFor[A]): AvroCodec[A] = withSchema(schemaFor.schema)
  override def encode(value: A): AnyRef = avroEncoder.encode(value)
  override def decode(value: Any): A = avroDecoder.decode(value)

  def toRecord(value: A): Record = encode(value) match {
    case record: Record => record
    case other          => sys.error(s"${other.getClass.getName} is not com.sksamuel.avro4s.Record")
  }
  def fromRecord(value: IndexedRecord): A = avroDecoder.decode(value)

  def recordOf(value: A): Option[Record] = Try(toRecord(value)).toOption

  /** https://avro.apache.org/docs/current/spec.html the grammar for a namespace is:
    *
    * <empty> | <name>[(<dot><name>)*]
    *
    * empty namespace is not allowed
    */
  private type Namespace = MatchesRegex["^[a-zA-Z0-9_.]+$"]
  def withNamespace(namespace: String Refined Namespace): AvroCodec[A] =
    withSchema(replaceNamespace(schema, namespace.value))

  def withoutNamespace: AvroCodec[A] = withSchema(removeNamespace(schema))
  def withoutDefaultField: AvroCodec[A] = withSchema(removeDefaultField(schema))
  def withoutDoc: AvroCodec[A] = withSchema(removeDocField(schema))
}

object AvroCodec {
  def apply[A](sf: SchemaFor[A], dc: AvroDecoder[A], ec: AvroEncoder[A]): AvroCodec[A] =
    new AvroCodec[A](sf, DecoderHelpers.buildWithSchema(dc, sf), EncoderHelpers.buildWithSchema(ec, sf))

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
      throw new Exception(s"incompatible schema: $msg")
    }
  }

  def apply[A: AvroDecoder: AvroEncoder: SchemaFor](schemaText: String): AvroCodec[A] =
    apply[A]((new Schema.Parser).parse(schemaText))
}
