package com.github.chenharryhua.nanjin.kafka

import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.sksamuel.avro4s.Decoder
import fs2.kafka.ProducerRecord
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerializer
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.kafka.streams.scala.serialization.Serdes

import scala.jdk.CollectionConverters.MapHasAsJava

final class PushGenericRecord(
  topicName: TopicName,
  keySchema: Schema,
  valSchema: Schema,
  srs: SchemaRegistrySettings)
    extends Serializable {

  private val topic: String = topicName.value

  private val keySer: AnyRef => Array[Byte] =
    keySchema.getType match {
      case Schema.Type.RECORD =>
        val ser = new GenericAvroSerializer()
        ser.configure(srs.config.asJava, true)
        (_: AnyRef) match {
          case gr: GenericRecord => ser.serialize(topic, gr)
          case data              => throw new Exception(s"key: $data is not a Generic Record")
        }

      case Schema.Type.STRING =>
        val ser = Serdes.stringSerde.serializer()
        (data: AnyRef) => ser.serialize(topic, Decoder[String].decode(data))
      case Schema.Type.INT =>
        val ser = Serdes.intSerde.serializer()
        (data: AnyRef) => ser.serialize(topic, Decoder[Int].decode(data))
      case Schema.Type.LONG =>
        val ser = Serdes.longSerde.serializer()
        (data: AnyRef) => ser.serialize(topic, Decoder[Long].decode(data))
      case Schema.Type.FLOAT =>
        val ser = Serdes.floatSerde.serializer()
        (data: AnyRef) => ser.serialize(topic, Decoder[Float].decode(data))
      case Schema.Type.DOUBLE =>
        val ser = Serdes.doubleSerde.serializer()
        (data: AnyRef) => ser.serialize(topic, Decoder[Double].decode(data))
      case _ => throw new Exception(s"unsupported key schema $keySchema")
    }

  private val valSer: AnyRef => Array[Byte] =
    valSchema.getType match {
      case Schema.Type.RECORD =>
        val ser = new GenericAvroSerializer()
        ser.configure(srs.config.asJava, false)
        (_: AnyRef) match {
          case gr: GenericRecord => ser.serialize(topic, gr)
          case data              => throw new Exception(s"value: $data is not a Generic Record")
        }

      case Schema.Type.STRING =>
        val ser = Serdes.stringSerde.serializer()
        (data: AnyRef) => ser.serialize(topic, Decoder[String].decode(data))
      case Schema.Type.INT =>
        val ser = Serdes.intSerde.serializer()
        (data: AnyRef) => ser.serialize(topic, Decoder[Int].decode(data))
      case Schema.Type.LONG =>
        val ser = Serdes.longSerde.serializer()
        (data: AnyRef) => ser.serialize(topic, Decoder[Long].decode(data))
      case Schema.Type.FLOAT =>
        val ser = Serdes.floatSerde.serializer()
        (data: AnyRef) => ser.serialize(topic, Decoder[Float].decode(data))
      case Schema.Type.DOUBLE =>
        val ser = Serdes.doubleSerde.serializer()
        (data: AnyRef) => ser.serialize(topic, Decoder[Double].decode(data))
      case _ => throw new Exception(s"unsupported value schema $valSchema")
    }

  def fromGenericRecord(gr: GenericRecord): ProducerRecord[Array[Byte], Array[Byte]] = {
    val key   = gr.get("key")
    val value = gr.get("value")

    (GenericData.get().validate(keySchema, key), GenericData.get().validate(valSchema, value)) match {
      case (true, true)   => ProducerRecord(topic, keySer(key), valSer(value))
      case (true, false)  => throw new Exception("invalid value")
      case (false, true)  => throw new Exception("invalid key")
      case (false, false) => throw new Exception("invalid both key and value")
    }
  }
}
