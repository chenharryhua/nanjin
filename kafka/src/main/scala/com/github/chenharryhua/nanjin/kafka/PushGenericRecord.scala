package com.github.chenharryhua.nanjin.kafka

import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.messages.kafka.codec.GRCodec
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

  @transient private lazy val keySer: AnyRef => Array[Byte] =
    pair.key.getType match {
      case Schema.Type.RECORD =>
        val ser = new GenericAvroSerializer()
        ser.configure(srs.config.asJava, true)
        val keyCodec = GRCodec(pair.key)
        (data: AnyRef) => ser.serialize(topic, keyCodec.decode(data))

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

      case _ => throw new Exception(s"unsupported key schema ${pair.key}")
    }

  @transient private lazy val valSer: AnyRef => Array[Byte] =
    pair.value.getType match {
      case Schema.Type.RECORD =>
        val ser = new GenericAvroSerializer()
        ser.configure(srs.config.asJava, false)
        val valCodec = GRCodec(pair.value)
        (data: AnyRef) => ser.serialize(topic, valCodec.decode(data))

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

      case _ => throw new Exception(s"unsupported value schema ${pair.value}")
    }

  def fromGenericRecord(gr: GenericRecord): ProducerRecord[Array[Byte], Array[Byte]] = {
    val key   = gr.get("key")
    val value = gr.get("value")

    ProducerRecord(topic, keySer(key), valSer(value))
  }
}
