package com.github.chenharryhua.nanjin.kafka

import io.confluent.kafka.streams.serdes.avro.GenericAvroDeserializer
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.streams.scala.serialization.Serdes

import scala.jdk.CollectionConverters.MapHasAsJava

final class BuildGenericRecord(
  topic: String,
  keySchema: Schema,
  valSchema: Schema,
  srs: SchemaRegistrySettings) extends Serializable{
  val schema: Schema = SchemaBuilder
    .builder("nj.kafka")
    .record("NJConsumerRecord")
    .fields()
    .requiredString("topic")
    .requiredInt("partition")
    .requiredLong("offset")
    .requiredLong("timestamp")
    .requiredInt("timestampType")
    .name("key")
    .`type`(keySchema)
    .noDefault()
    .name("value")
    .`type`(valSchema)
    .noDefault()
    .name("headers")
    .`type`()
    .array()
    .items(SchemaBuilder.record("NJHeader").fields().requiredString("key").requiredBytes("value").endRecord())
    .noDefault()
    .endRecord()

  private val keyDecode: Array[Byte] => Any = (data: Array[Byte]) =>
    keySchema.getType match {
      case Schema.Type.RECORD =>
        val deser = new GenericAvroDeserializer()
        deser.configure(srs.config.asJava, true)
        deser.deserialize(topic, data)
      case Schema.Type.STRING => Serdes.stringSerde.deserializer().deserialize(topic, data)
      case Schema.Type.BYTES  => Serdes.bytesSerde.deserializer().deserialize(topic, data)
      case Schema.Type.INT    => Serdes.intSerde.deserializer().deserialize(topic, data)
      case Schema.Type.LONG   => Serdes.longSerde.deserializer().deserialize(topic, data)
      case Schema.Type.FLOAT  => Serdes.floatSerde.deserializer().deserialize(topic, data)
      case Schema.Type.DOUBLE => Serdes.doubleSerde.deserializer().deserialize(topic, data)
      case _                  => throw new Exception(s"unsupported schema $keySchema")
    }
  private val valDecode: Array[Byte] => Any = (data: Array[Byte]) =>
    valSchema.getType match {
      case Schema.Type.RECORD =>
        val deser = new GenericAvroDeserializer()
        deser.configure(srs.config.asJava, false)
        deser.deserialize(topic, data)
      case Schema.Type.STRING => Serdes.stringSerde.deserializer().deserialize(topic, data)
      case Schema.Type.BYTES  => Serdes.bytesSerde.deserializer().deserialize(topic, data)
      case Schema.Type.INT    => Serdes.intSerde.deserializer().deserialize(topic, data)
      case Schema.Type.LONG   => Serdes.longSerde.deserializer().deserialize(topic, data)
      case Schema.Type.FLOAT  => Serdes.floatSerde.deserializer().deserialize(topic, data)
      case Schema.Type.DOUBLE => Serdes.doubleSerde.deserializer().deserialize(topic, data)
      case _                  => throw new Exception(s"unsupported schema $valSchema")
    }

  def toGenericRecord(ccr: ConsumerRecord[Array[Byte], Array[Byte]]): GenericRecord = {
    val record = new GenericData.Record(schema)
    record.put("topic", ccr.topic)
    record.put("partition", ccr.partition)
    record.put("offset", ccr.offset)
    record.put("timestamp", ccr.timestamp())
    record.put("timestampType", ccr.timestampType().id)
    record.put("key", keyDecode(ccr.key))
    record.put("value", valDecode(ccr.value))
   // record.put("headers", ccr.headers().toArray)
    record
  }
}
