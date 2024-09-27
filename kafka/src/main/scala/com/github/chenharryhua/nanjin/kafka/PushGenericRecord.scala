package com.github.chenharryhua.nanjin.kafka

import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.messages.kafka.codec.immigrate
import com.sksamuel.avro4s.Decoder
import fs2.kafka.ProducerRecord
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerializer
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.streams.scala.serialization.Serdes

import scala.jdk.CollectionConverters.MapHasAsJava

final class PushGenericRecord(srs: SchemaRegistrySettings, topicName: TopicName, pair: AvroSchemaPair)
    extends Serializable {

  private val topic: String = topicName.value

  @transient private lazy val key_serialize: AnyRef => Array[Byte] =
    pair.key.getType match {
      case Schema.Type.RECORD =>
        val ser = new GenericAvroSerializer()
        ser.configure(srs.config.asJava, true)
        // java world
        (data: AnyRef) => ser.serialize(topic, immigrate(pair.key, data.asInstanceOf[GenericRecord]).get)

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
      case Schema.Type.BYTES =>
        val ser = Serdes.byteArraySerde.serializer()
        (data: AnyRef) => ser.serialize(topic, Decoder[Array[Byte]].decode(data))

      case _ => throw new Exception(s"unsupported key schema: ${pair.key.toString}")
    }

  @transient private lazy val val_serialize: AnyRef => Array[Byte] =
    pair.value.getType match {
      case Schema.Type.RECORD =>
        val ser = new GenericAvroSerializer()
        ser.configure(srs.config.asJava, false)
        // java world
        (data: AnyRef) => ser.serialize(topic, immigrate(pair.value, data.asInstanceOf[GenericRecord]).get)

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
      case Schema.Type.BYTES =>
        val ser = Serdes.byteArraySerde.serializer()
        (data: AnyRef) => ser.serialize(topic, Decoder[Array[Byte]].decode(data))

      case _ => throw new Exception(s"unsupported value schema: ${pair.value.toString}")
    }

  def fromGenericRecord(gr: GenericRecord): ProducerRecord[Array[Byte], Array[Byte]] = {
    val key   = gr.get("key")
    val value = gr.get("value")

    ProducerRecord(topic, key_serialize(key), val_serialize(value))
  }
}
