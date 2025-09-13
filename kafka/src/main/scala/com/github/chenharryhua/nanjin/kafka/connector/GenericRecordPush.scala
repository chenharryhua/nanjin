package com.github.chenharryhua.nanjin.kafka.connector

import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.{AvroSchemaPair, SchemaRegistrySettings}
import com.github.chenharryhua.nanjin.messages.kafka.codec.immigrate
import com.sksamuel.avro4s.Decoder
import fs2.kafka.ProducerRecord
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerializer
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.streams.scala.serialization.Serdes

import scala.jdk.CollectionConverters.MapHasAsJava

final private[kafka] class GenericRecordPush(
  srs: SchemaRegistrySettings,
  topicName: TopicName,
  pair: AvroSchemaPair) {
  val schema: Schema = pair.consumerSchema

  private val topic: String = topicName.value

  private val key_serialize: AnyRef => Array[Byte] =
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

      case _ => throw new RuntimeException(s"unsupported key schema: ${pair.key.toString}")
    }

  private val val_serialize: AnyRef => Array[Byte] =
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

      case _ => throw new RuntimeException(s"unsupported value schema: ${pair.value.toString}")
    }

  /** @param gr
    *   a GenericRecord of NJConsumerRecord
    * @return
    */
  def fromGenericRecord(gr: GenericRecord): ProducerRecord[Array[Byte], Array[Byte]] =
    ProducerRecord(topic, key_serialize(gr.get("key")), val_serialize(gr.get("value")))
}
