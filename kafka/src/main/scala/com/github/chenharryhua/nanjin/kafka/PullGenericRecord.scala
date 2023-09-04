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
import org.apache.avro.generic.{GenericData, GenericDatumWriter}
import org.apache.avro.io.{BinaryEncoder, EncoderFactory, JsonEncoder}
import org.apache.kafka.streams.scala.serialization.Serdes

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import scala.jdk.CollectionConverters.{MapHasAsJava, SeqHasAsJava}
import scala.util.{Failure, Success, Try, Using}

final class PullGenericRecord(srs: SchemaRegistrySettings, topicName: TopicName, pair: AvroSchemaPair)
    extends Serializable {
  private val schema: Schema = pair.consumerRecordSchema
  private val topic: String  = topicName.value

  @transient private lazy val keyDecode: Array[Byte] => Any =
    pair.key.getType match {
      case Schema.Type.RECORD =>
        val deser = new GenericAvroDeserializer()
        deser.configure(srs.config.asJava, true)
        (data: Array[Byte]) => deser.deserialize(topic, data)
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
        (data: Array[Byte]) => deser.deserialize(topic, data)
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

  def toRecord(ccr: KafkaByteConsumerRecord): GenericData.Record = {
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
    record.put("key", Try(keyDecode(ccr.key)).getOrElse(null))
    record.put("value", Try(valDecode(ccr.value)).getOrElse(null))
    record
  }

  def toRecord(ccr: ConsumerRecord[Array[Byte], Array[Byte]]): GenericData.Record =
    toRecord(ccr.transformInto[KafkaByteConsumerRecord])

  @transient private lazy val datumWriter: GenericDatumWriter[GenericData.Record] =
    new GenericDatumWriter[GenericData.Record](schema)

  def toJacksonString(ccr: KafkaByteConsumerRecord): Either[GenericData.Record, String] = {
    val gr: GenericData.Record = toRecord(ccr)
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

  def toJacksonString(ccr: ConsumerRecord[Array[Byte], Array[Byte]]): Either[GenericData.Record, String] =
    toJacksonString(ccr.transformInto[KafkaByteConsumerRecord])

  def toBinAvro(ccr: KafkaByteConsumerRecord): Chunk[Byte] =
    Using(new ByteArrayOutputStream) { baos =>
      val gr: GenericData.Record = toRecord(ccr)
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
