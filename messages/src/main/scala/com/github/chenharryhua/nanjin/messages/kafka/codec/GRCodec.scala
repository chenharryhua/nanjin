package com.github.chenharryhua.nanjin.messages.kafka.codec

import com.sksamuel.avro4s.*
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}

import java.io.ByteArrayOutputStream
import scala.util.{Try, Using}

object GRCodec {

  /** using the schema to reshape the input generic record
    *
    * @param schema
    *   target schema
    * @param gr
    *   input generic record
    * @return
    */
  private def reshape(schema: Schema, gr: GenericRecord): Try[GenericRecord] =
    Using(new ByteArrayOutputStream()) { baos =>
      val encoder = EncoderFactory.get().binaryEncoder(baos, null)
      new GenericDatumWriter[GenericRecord](gr.getSchema).write(gr, encoder)
      encoder.flush()

      val decoder = DecoderFactory.get().binaryDecoder(baos.toByteArray, null)
      new GenericDatumReader[GenericRecord](gr.getSchema, schema).read(null, decoder)
    }(_.close())

  def apply(skm: Schema): NJAvroCodec[GenericRecord] = {
    val skmFor: SchemaFor[GenericRecord] = new SchemaFor[GenericRecord] {
      override def schema: Schema           = skm
      override def fieldMapper: FieldMapper = DefaultFieldMapper
    }

    val encoder: Encoder[GenericRecord] = new Encoder[GenericRecord] {
      override def encode(value: GenericRecord): GenericRecord = reshape(skm, value).get
      override def schemaFor: SchemaFor[GenericRecord]         = skmFor
    }

    val decoder: Decoder[GenericRecord] = new Decoder[GenericRecord] {
      override def decode(value: Any): GenericRecord = value match {
        case gr: GenericRecord => reshape(skm, gr).get
        case other             => sys.error(s"$other is not a generic record")
      }

      override def schemaFor: SchemaFor[GenericRecord] = skmFor
    }

    NJAvroCodec[GenericRecord](skmFor, decoder, encoder)
  }
}
