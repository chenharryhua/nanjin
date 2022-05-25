package com.github.chenharryhua.nanjin.messages.kafka.codec

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.sksamuel.avro4s.{Codec, FieldMapper, SchemaFor}
import io.circe.{Encoder as JsonEncoder, Json}
import io.circe.jackson.jacksonToCirce
import io.confluent.kafka.streams.serdes.avro.GenericAvroDeserializer
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{EncoderFactory, JsonEncoder as AvroJsonEncoder}
import org.apache.kafka.common.serialization.{Deserializer, Serializer}

import java.io.ByteArrayOutputStream
import java.util

final class KUnknown private (val value: String) extends Serializable {
  def asJackson: JsonNode = new ObjectMapper().readTree(value)
  def asJson: Json        = jacksonToCirce(asJackson)

  override def toString: String = value
}

object KUnknown {
  implicit val kunknownJsonEncoder: JsonEncoder[KUnknown] = _.asJson

  implicit val avroKUnknownSchemaFor: SchemaFor[KUnknown] = new SchemaFor[KUnknown] {
    override val schema: Schema           = SchemaFor[String].schema
    override val fieldMapper: FieldMapper = SchemaFor[String].fieldMapper
  }

  implicit val kunknownSerde: SerdeOf[KUnknown] = new SerdeOf[KUnknown] {
    // allow serialize to disk, forbid read back
    private val codec: Codec[KUnknown] = new Codec[KUnknown] {
      override def encode(value: KUnknown): String = value.value
      override def decode(value: Any): KUnknown    = sys.error("KUknown is not allowed to do decode because schema is unknown")
      override def schemaFor: SchemaFor[KUnknown]  = avroKUnknownSchemaFor
    }
    override val avroCodec: NJAvroCodec[KUnknown] = NJAvroCodec(codec.schemaFor, codec, codec)

    // allow read from kafka, forbid send back
    override val serializer: Serializer[KUnknown] = new Serializer[KUnknown] with Serializable {
      override def serialize(topic: String, data: KUnknown): Array[Byte] =
        sys.error("KUknown is not allowed to do serialization because schema is unknown")
    }

    override val deserializer: Deserializer[KUnknown] = new Deserializer[KUnknown] with Serializable {

      @transient private[this] lazy val deSer: GenericAvroDeserializer = new GenericAvroDeserializer
      override def close(): Unit                                       = deSer.close()
      override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit =
        deSer.configure(configs, isKey)

      @transient private[this] lazy val gdw: GenericDatumWriter[GenericRecord] = new GenericDatumWriter[GenericRecord]

      override def deserialize(topic: String, data: Array[Byte]): KUnknown = {
        val gr: GenericRecord = deSer.deserialize(topic, data)
        gdw.setSchema(gr.getSchema)
        val baos: ByteArrayOutputStream = new ByteArrayOutputStream
        val encoder: AvroJsonEncoder    = EncoderFactory.get().jsonEncoder(gr.getSchema, baos)
        gdw.write(gr, encoder)
        encoder.flush()
        baos.close()
        new KUnknown(baos.toString)
      }
    }
  }
}
