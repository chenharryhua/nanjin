package com.github.chenharryhua.nanjin.kafka

import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.messages.kafka.NJHeader
import com.github.chenharryhua.nanjin.messages.kafka.instances.*
import com.sksamuel.avro4s.SchemaFor
import fs2.Chunk
import fs2.kafka.{ConsumerRecord, KafkaByteConsumerRecord}
import io.confluent.kafka.streams.serdes.avro.GenericAvroDeserializer
import io.scalaland.chimney.dsl.TransformerOps
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{BinaryEncoder, EncoderFactory, JsonEncoder}
import org.apache.kafka.streams.scala.serialization.Serdes

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import scala.jdk.CollectionConverters.{MapHasAsJava, SeqHasAsJava}
import scala.util.{Failure, Success, Using}

final class PullGenericRecord(srs: SchemaRegistrySettings, topicName: TopicName, pair: AvroSchemaPair)
    extends Serializable {
  private val schema: Schema = pair.consumerRecordSchema
  private val topic: String  = topicName.value

  @transient private lazy val keyDecode: Array[Byte] => Any =
    pair.key.getType match {
      case Schema.Type.RECORD =>
        val deser = new GenericAvroDeserializer()
        deser.configure(srs.config.asJava, true)
        (data: Array[Byte]) =>
          // Error retrieving Avro key schema for id
          try deser.deserialize(topic, data)
          catch { case _: Throwable => null }
      case Schema.Type.STRING =>
        val deser = Serdes.stringSerde.deserializer()
        (data: Array[Byte]) => deser.deserialize(topic, data)
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
      case Schema.Type.BYTES =>
        val deser = Serdes.byteArraySerde.deserializer()
        (data: Array[Byte]) => deser.deserialize(topic, data)

      case _ => throw new Exception(s"unsupported key schema ${pair.key}")
    }

  @transient private lazy val valDecode: Array[Byte] => Any =
    pair.value.getType match {
      case Schema.Type.RECORD =>
        val deser = new GenericAvroDeserializer()
        deser.configure(srs.config.asJava, false)
        (data: Array[Byte]) =>
          // Error retrieving Avro key schema for id
          try deser.deserialize(topic, data)
          catch { case _: Throwable => null }
      case Schema.Type.STRING =>
        val deser = Serdes.stringSerde.deserializer()
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
      case Schema.Type.BYTES =>
        val deser = Serdes.byteArraySerde.deserializer()
        (data: Array[Byte]) => deser.deserialize(topic, data)

      case _ => throw new Exception(s"unsupported value schema ${pair.value}")
    }

  def toGenericRecord(ccr: KafkaByteConsumerRecord): GenericRecord = {
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
    record.put("headers", headers.toList.asJava)
    record.put("key", keyDecode(ccr.key))
    record.put("value", valDecode(ccr.value))
    record
  }

  def toGenericRecord(ccr: ConsumerRecord[Array[Byte], Array[Byte]]): GenericRecord =
    toGenericRecord(ccr.transformInto[KafkaByteConsumerRecord])

  @transient private lazy val datumWriter: GenericDatumWriter[GenericRecord] =
    new GenericDatumWriter[GenericRecord](schema)

  def toJacksonString(ccr: KafkaByteConsumerRecord): Either[GenericRecord, String] = {
    val gr: GenericRecord = toGenericRecord(ccr)
    Using(new ByteArrayOutputStream) { baos =>
      val encoder: JsonEncoder = EncoderFactory.get().jsonEncoder(schema, baos)
      datumWriter.write(gr, encoder)
      encoder.flush()
      baos.toString(StandardCharsets.UTF_8)
    }(_.close()) match {
      case Failure(_)     => Left(gr)
      case Success(value) => Right(value)
    }
  }

  def toJacksonString(ccr: ConsumerRecord[Array[Byte], Array[Byte]]): Either[GenericRecord, String] =
    toJacksonString(ccr.transformInto[KafkaByteConsumerRecord])

  def toBinAvro(ccr: KafkaByteConsumerRecord): Chunk[Byte] =
    Using(new ByteArrayOutputStream) { baos =>
      val gr: GenericRecord      = toGenericRecord(ccr)
      val encoder: BinaryEncoder = EncoderFactory.get().binaryEncoder(baos, null)
      datumWriter.write(gr, encoder)
      encoder.flush()
      Chunk.from(baos.toByteArray)
    }(_.close()) match {
      case Failure(_)     => Chunk.empty[Byte]
      case Success(value) => value
    }

  def toBinAvro(ccr: ConsumerRecord[Array[Byte], Array[Byte]]): Chunk[Byte] =
    toBinAvro(ccr.transformInto[KafkaByteConsumerRecord])

}
