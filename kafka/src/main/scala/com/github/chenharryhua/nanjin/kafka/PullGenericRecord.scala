package com.github.chenharryhua.nanjin.kafka

import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.messages.kafka.{NJConsumerRecord, NJHeader}
import com.sksamuel.avro4s.SchemaFor
import io.circe.Json
import io.circe.parser.parse
import io.confluent.kafka.streams.serdes.avro.GenericAvroDeserializer
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{EncoderFactory, JsonEncoder}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.streams.scala.serialization.Serdes

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import scala.jdk.CollectionConverters.{MapHasAsJava, SeqHasAsJava}
import scala.util.Try

final class PullGenericRecord(
  topicName: TopicName,
  keySchema: Schema,
  valSchema: Schema,
  srs: SchemaRegistrySettings)
    extends Serializable {

  private val schema: Schema = NJConsumerRecord.schema(keySchema, valSchema)
  private val topic: String  = topicName.value

  private val keyDecode: Array[Byte] => Any =
    keySchema.getType match {
      case Schema.Type.RECORD =>
        val deser = new GenericAvroDeserializer()
        deser.configure(srs.config.asJava, true)
        (data: Array[Byte]) => deser.deserialize(topic, data)
      case Schema.Type.STRING =>
        val deser = Serdes.stringSerde.deserializer()
        (data: Array[Byte]) => deser.deserialize(topic, data)
      case Schema.Type.BYTES =>
        val keyDeser = Serdes.bytesSerde.deserializer()
        (data: Array[Byte]) => keyDeser.deserialize(topic, data)
      case Schema.Type.INT =>
        val deser = Serdes.intSerde.deserializer()
        (data: Array[Byte]) => deser.deserialize(topic, data)
      case Schema.Type.LONG =>
        val deser = Serdes.longSerde.deserializer()
        (data: Array[Byte]) => deser.deserialize(topic, data)
      case Schema.Type.FLOAT =>
        val keyDeser = Serdes.floatSerde.deserializer()
        (data: Array[Byte]) => keyDeser.deserialize(topic, data)
      case Schema.Type.DOUBLE =>
        val deser = Serdes.doubleSerde.deserializer()
        (data: Array[Byte]) => deser.deserialize(topic, data)
      case _ => throw new Exception(s"unsupported key schema $keySchema")
    }
  private val valDecode: Array[Byte] => Any =
    valSchema.getType match {
      case Schema.Type.RECORD =>
        val deser = new GenericAvroDeserializer()
        deser.configure(srs.config.asJava, false)
        (data: Array[Byte]) => deser.deserialize(topic, data)
      case Schema.Type.STRING =>
        val deser = Serdes.stringSerde.deserializer()
        (data: Array[Byte]) => deser.deserialize(topic, data)
      case Schema.Type.BYTES =>
        val deser = Serdes.bytesSerde.deserializer()
        (data: Array[Byte]) => deser.deserialize(topic, data)
      case Schema.Type.INT =>
        val deser = Serdes.intSerde.deserializer()
        (data: Array[Byte]) => deser.deserialize(topic, data)
      case Schema.Type.LONG =>
        val deser = Serdes.longSerde.deserializer()
        (data: Array[Byte]) => deser.deserialize(topic, data)
      case Schema.Type.FLOAT =>
        val deser = Serdes.floatSerde.deserializer()
        (data: Array[Byte]) => deser.deserialize(topic, data)
      case Schema.Type.DOUBLE =>
        val deser = Serdes.doubleSerde.deserializer()
        (data: Array[Byte]) => deser.deserialize(topic, data)
      case _ => throw new Exception(s"unsupported value schema $valSchema")
    }

  private def toGenericRecord(ccr: ConsumerRecord[Array[Byte], Array[Byte]]): GenericRecord = {
    val record: GenericData.Record = new GenericData.Record(schema)
    val headers: Array[GenericData.Record] = ccr.headers().toArray.map { h =>
      val header = new GenericData.Record(SchemaFor[NJHeader].schema)
      header.put("key", h.key())
      header.put("value", ByteBuffer.wrap(h.value()))
      header
    }
    record.put("topic", ccr.topic)
    record.put("partition", ccr.partition)
    record.put("offset", ccr.offset)
    record.put("timestamp", ccr.timestamp())
    record.put("timestampType", ccr.timestampType().id)
    record.put("key", Try(keyDecode(ccr.key)).getOrElse(null))
    record.put("value", Try(valDecode(ccr.value)).getOrElse(null))
    record.put("headers", headers.toList.asJava)
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
    parse(baos.toString(StandardCharsets.UTF_8)) match {
      case Left(value)  => throw value // should never happen
      case Right(value) => value
    }
  }
}
