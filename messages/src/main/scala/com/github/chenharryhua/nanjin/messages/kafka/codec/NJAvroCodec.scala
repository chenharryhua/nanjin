package com.github.chenharryhua.nanjin.messages.kafka.codec

import com.sksamuel.avro4s.{
  Codec,
  Decoder as AvroDecoder,
  DecoderHelpers,
  Encoder as AvroEncoder,
  EncoderHelpers,
  SchemaFor
}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.string.MatchesRegex
import org.apache.avro.Schema
final class NJAvroCodec[A] private (
  val schemaFor: SchemaFor[A],
  avroDecoder: AvroDecoder[A],
  avroEncoder: AvroEncoder[A])
    extends Codec[A] {

  def idConversion(a: A): A = avroDecoder.decode(avroEncoder.encode(a))

  def withSchema(schema: Schema): NJAvroCodec[A] =
    NJAvroCodec(schema)(avroDecoder, avroEncoder, schemaFor)

  override def withSchema(schemaFor: SchemaFor[A]): NJAvroCodec[A] = withSchema(schemaFor.schema)
  override def encode(value: A): AnyRef                            = avroEncoder.encode(value)
  override def decode(value: Any): A                               = avroDecoder.decode(value)

  /** https://avro.apache.org/docs/current/spec.html the grammar for a namespace is:
    *
    * <empty> | <name>[(<dot><name>)*]
    *
    * empty namespace is not allowed
    */
  private type Namespace = MatchesRegex["^[a-zA-Z0-9_.]+$"]
  def withNamespace(namespace: String Refined Namespace): NJAvroCodec[A] =
    withSchema(replaceNamespace(schema, namespace.value))

  def withoutNamespace: NJAvroCodec[A]    = withSchema(removeNamespace(schema))
  def withoutDefaultField: NJAvroCodec[A] = withSchema(removeDefaultField(schema))
}

object NJAvroCodec {
  def apply[A](sf: SchemaFor[A], dc: AvroDecoder[A], ec: AvroEncoder[A]): NJAvroCodec[A] =
    new NJAvroCodec[A](sf, DecoderHelpers.buildWithSchema(dc, sf), EncoderHelpers.buildWithSchema(ec, sf))

  def apply[A](implicit dc: AvroDecoder[A], ec: AvroEncoder[A], sf: SchemaFor[A]): NJAvroCodec[A] =
    apply[A](sf, dc, ec)

  def apply[A](
    schema: Schema)(implicit dc: AvroDecoder[A], ec: AvroEncoder[A], sf: SchemaFor[A]): NJAvroCodec[A] = {
    val b = backwardCompatibility(sf.schema, schema)
    val f = forwardCompatibility(sf.schema, schema)
    if (b.isEmpty && f.isEmpty) {
      apply(SchemaFor[A](schema), dc, ec)
    } else {
      val msg = (b ++ f).map(_.getMessage).mkString("\n")
      throw new Exception(msg)
    }
  }

  def apply[A: AvroDecoder: AvroEncoder: SchemaFor](schemaText: String): NJAvroCodec[A] =
    apply[A]((new Schema.Parser).parse(schemaText))
}
