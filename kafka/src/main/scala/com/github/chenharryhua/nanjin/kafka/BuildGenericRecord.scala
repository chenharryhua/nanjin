package com.github.chenharryhua.nanjin.kafka

import com.github.chenharryhua.nanjin.messages.kafka.NJConsumerRecord
import com.sksamuel.avro4s.{DefaultFieldMapper, FieldMapper, SchemaFor}
import io.circe.Json
import io.circe.parser.parse
import io.confluent.kafka.streams.serdes.avro.GenericAvroDeserializer
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{EncoderFactory, JsonEncoder}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.streams.scala.serialization.Serdes

import java.io.ByteArrayOutputStream
import java.util.Collections
import scala.annotation.nowarn
import scala.jdk.CollectionConverters.MapHasAsJava

final class BuildGenericRecord(
  topic: String,
  keySchema: Schema,
  valSchema: Schema,
  srs: SchemaRegistrySettings)
    extends Serializable {

  private case class KEY()
  private case class VAL()

  @nowarn
  implicit private val schemaForKey: SchemaFor[KEY] = new SchemaFor[KEY] {
    override def schema: Schema           = keySchema
    override def fieldMapper: FieldMapper = DefaultFieldMapper
  }

  @nowarn
  implicit private val schemaForVal: SchemaFor[VAL] = new SchemaFor[VAL] {
    override def schema: Schema           = valSchema
    override def fieldMapper: FieldMapper = DefaultFieldMapper
  }

  private val schema: Schema = SchemaFor[NJConsumerRecord[KEY, VAL]].schema

  private val keyDecode: Array[Byte] => Any =
    keySchema.getType match {
      case Schema.Type.RECORD =>
        val keyDeser = new GenericAvroDeserializer()
        keyDeser.configure(srs.config.asJava, true)
        (data: Array[Byte]) => keyDeser.deserialize(topic, data)
      case Schema.Type.STRING =>
        val keyDeser = Serdes.stringSerde.deserializer()
        (data: Array[Byte]) => keyDeser.deserialize(topic, data)
      case Schema.Type.BYTES =>
        val keyDeser = Serdes.bytesSerde.deserializer()
        (data: Array[Byte]) => keyDeser.deserialize(topic, data)
      case Schema.Type.INT =>
        val keyDeser = Serdes.intSerde.deserializer()
        (data: Array[Byte]) => keyDeser.deserialize(topic, data)
      case Schema.Type.LONG =>
        val keyDeser = Serdes.longSerde.deserializer()
        (data: Array[Byte]) => keyDeser.deserialize(topic, data)
      case Schema.Type.FLOAT =>
        val keyDeser = Serdes.floatSerde.deserializer()
        (data: Array[Byte]) => keyDeser.deserialize(topic, data)
      case Schema.Type.DOUBLE =>
        val keyDeser = Serdes.doubleSerde.deserializer()
        (data: Array[Byte]) => keyDeser.deserialize(topic, data)
      case _ => throw new Exception(s"unsupported schema $keySchema")
    }
  private val valDecode: Array[Byte] => Any =
    valSchema.getType match {
      case Schema.Type.RECORD =>
        val valDeser = new GenericAvroDeserializer()
        valDeser.configure(srs.config.asJava, false)
        (data: Array[Byte]) => valDeser.deserialize(topic, data)
      case Schema.Type.STRING =>
        val valDeser = Serdes.stringSerde.deserializer()
        (data: Array[Byte]) => valDeser.deserialize(topic, data)
      case Schema.Type.BYTES =>
        val valDeser = Serdes.bytesSerde.deserializer()
        (data: Array[Byte]) => valDeser.deserialize(topic, data)
      case Schema.Type.INT =>
        val valDeser = Serdes.intSerde.deserializer()
        (data: Array[Byte]) => valDeser.deserialize(topic, data)
      case Schema.Type.LONG =>
        val valDeser = Serdes.longSerde.deserializer()
        (data: Array[Byte]) => valDeser.deserialize(topic, data)
      case Schema.Type.FLOAT =>
        val valDeser = Serdes.floatSerde.deserializer()
        (data: Array[Byte]) => valDeser.deserialize(topic, data)
      case Schema.Type.DOUBLE =>
        val valDeser = Serdes.doubleSerde.deserializer()
        (data: Array[Byte]) => valDeser.deserialize(topic, data)
      case _ => throw new Exception(s"unsupported schema $valSchema")
    }

  private def toGenericRecord(ccr: ConsumerRecord[Array[Byte], Array[Byte]]): GenericRecord = {
    val record = new GenericData.Record(schema)
    record.put("topic", ccr.topic)
    record.put("partition", ccr.partition)
    record.put("offset", ccr.offset)
    record.put("timestamp", ccr.timestamp())
    record.put("timestampType", ccr.timestampType().id)
    record.put("key", keyDecode(ccr.key))
    record.put("value", valDecode(ccr.value))
    record.put("headers", Collections.emptyList())
    record
  }

  private val datumWriter = new GenericDatumWriter[GenericRecord](schema)

  def toJson(ccr: ConsumerRecord[Array[Byte], Array[Byte]]): Json = {
    val gr: GenericRecord           = toGenericRecord(ccr)
    val baos: ByteArrayOutputStream = new ByteArrayOutputStream
    val encoder: JsonEncoder        = EncoderFactory.get().jsonEncoder(schema, baos)
    datumWriter.write(gr, encoder)
    encoder.flush()
    baos.close()
    parse(baos.toString()) match {
      case Left(value)  => throw value
      case Right(value) => value
    }
  }
}
