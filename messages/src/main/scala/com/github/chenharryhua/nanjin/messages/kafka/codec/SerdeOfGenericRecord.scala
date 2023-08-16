package com.github.chenharryhua.nanjin.messages.kafka.codec

import com.sksamuel.avro4s.{Codec, SchemaFor}
import io.confluent.kafka.streams.serdes.avro.{GenericAvroDeserializer, GenericAvroSerializer}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.{Deserializer, Serializer}

import java.util

object SerdeOfGenericRecord {
  def apply(skm: Schema): SerdeOf[GenericRecord] = new SerdeOf[GenericRecord] {
    private val codec: Codec[GenericRecord] = new Codec[GenericRecord] {
      override def encode(value: GenericRecord): GenericRecord = value

      override def decode(value: Any): GenericRecord = value match {
        case gr: GenericRecord => gr
        case _                 => sys.error(s"${value.toString} is not a GenericRecord")
      }

      override def schemaFor: SchemaFor[GenericRecord] = SchemaFor[GenericRecord](skm)
    }

    override def avroCodec: NJAvroCodec[GenericRecord] = NJAvroCodec(codec.schemaFor, codec, codec)

    override val serializer: Serializer[GenericRecord] =
      new Serializer[GenericRecord] with Serializable {
        @transient private[this] lazy val ser: GenericAvroSerializer = new GenericAvroSerializer()

        override def close(): Unit = ser.close()

        override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit =
          ser.configure(configs, isKey)

        override def serialize(topic: String, data: GenericRecord): Array[Byte] =
          ser.serialize(topic, data)

      }

    override val deserializer: Deserializer[GenericRecord] =
      new Deserializer[GenericRecord] with Serializable {
        @transient private[this] lazy val deSer: GenericAvroDeserializer = new GenericAvroDeserializer

        override def close(): Unit = deSer.close()

        override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit =
          deSer.configure(configs, isKey)

        override def deserialize(topic: String, data: Array[Byte]): GenericRecord =
          deSer.deserialize(topic, data)
      }
  }
}
