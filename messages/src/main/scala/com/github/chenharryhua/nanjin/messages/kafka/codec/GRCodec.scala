package com.github.chenharryhua.nanjin.messages.kafka.codec

import com.sksamuel.avro4s.*
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

object GRCodec {

  def apply(skm: Schema): NJAvroCodec[GenericRecord] = {
    val skmFor: SchemaFor[GenericRecord] = new SchemaFor[GenericRecord] {
      override val schema: Schema           = skm
      override val fieldMapper: FieldMapper = DefaultFieldMapper
    }

    val encoder: Encoder[GenericRecord] = new Encoder[GenericRecord] {
      override val schemaFor: SchemaFor[GenericRecord] = skmFor
      override def encode(value: GenericRecord): AnyRef =
        if (value == null) null else reshape(schema, value).get
    }

    val decoder: Decoder[GenericRecord] = new Decoder[GenericRecord] {
      override val schemaFor: SchemaFor[GenericRecord] = skmFor
      override def decode(value: Any): GenericRecord = value match {
        case gr: GenericRecord => reshape(schema, gr).get
        case null              => null
        case other             => sys.error(s"$other is not a generic record")
      }
    }

    NJAvroCodec[GenericRecord](skmFor, decoder, encoder)
  }
}
