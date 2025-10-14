package com.github.chenharryhua.nanjin.kafka.connector
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.AvroSchemaPair
import com.github.chenharryhua.nanjin.messages.kafka.instances.*
import com.github.chenharryhua.nanjin.messages.kafka.{CRMetaInfo, NJHeader}
import com.sksamuel.avro4s.SchemaFor
import fs2.kafka.{ConsumerRecord, KafkaByteConsumerRecord}
import io.circe.syntax.EncoderOps
import io.scalaland.chimney.dsl.TransformerOps
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumReader}
import org.apache.avro.io.DecoderFactory
import org.apache.kafka.streams.scala.serialization.Serdes

import java.nio.ByteBuffer
import scala.jdk.CollectionConverters.SeqHasAsJava
import scala.jdk.OptionConverters.RichOptional
import scala.util.{Failure, Success, Try}

final private class PullGenericRecord(topicName: TopicName, pair: AvroSchemaPair) {
  private val schema: Schema = pair.consumerSchema
  private val topic: String = topicName.value

  private val key_decode: Array[Byte] => Try[Any] =
    pair.key.rawSchema().getType match {
      case Schema.Type.RECORD =>
        val reader = new GenericDatumReader[GenericData.Record](pair.key.rawSchema())
        (data: Array[Byte]) =>
          if (data == null) Success(null)
          else
            Try { // drop 5: 1 byte magic, 4 bytes schema ID
              val decoder = DecoderFactory.get.binaryDecoder(data.drop(5), null)
              reader.read(null, decoder)
            }
      case Schema.Type.STRING =>
        val deser = Serdes.stringSerde.deserializer()
        (data: Array[Byte]) => Try(deser.deserialize(topic, data))
      case Schema.Type.INT =>
        val deser = Serdes.intSerde.deserializer()
        (data: Array[Byte]) => Try(deser.deserialize(topic, data))
      case Schema.Type.LONG =>
        val deser = Serdes.longSerde.deserializer()
        (data: Array[Byte]) => Try(deser.deserialize(topic, data))
      case Schema.Type.FLOAT =>
        val keyDeser = Serdes.floatSerde.deserializer()
        (data: Array[Byte]) => Try(keyDeser.deserialize(topic, data))
      case Schema.Type.DOUBLE =>
        val deser = Serdes.doubleSerde.deserializer()
        (data: Array[Byte]) => Try(deser.deserialize(topic, data))
      case Schema.Type.BYTES =>
        val deser = Serdes.byteArraySerde.deserializer()
        (data: Array[Byte]) => Try(deser.deserialize(topic, data))
      case Schema.Type.NULL =>
        (_: Array[Byte]) => Success(null)

      case _ => throw new RuntimeException(s"unsupported key schema: ${pair.key.toString}")
    }

  private val val_decode: Array[Byte] => Try[Any] =
    pair.value.rawSchema().getType match {
      case Schema.Type.RECORD =>
        val reader = new GenericDatumReader[GenericData.Record](pair.value.rawSchema())
        (data: Array[Byte]) =>
          if (data == null) Success(null)
          else
            Try { // drop 5: 1 byte magic, 4 bytes schema ID
              val decoder = DecoderFactory.get.binaryDecoder(data.drop(5), null)
              reader.read(null, decoder)
            }
      case Schema.Type.STRING =>
        val deser = Serdes.stringSerde.deserializer()
        (data: Array[Byte]) => Try(deser.deserialize(topic, data))
      case Schema.Type.INT =>
        val deser = Serdes.intSerde.deserializer()
        (data: Array[Byte]) => Try(deser.deserialize(topic, data))
      case Schema.Type.LONG =>
        val deser = Serdes.longSerde.deserializer()
        (data: Array[Byte]) => Try(deser.deserialize(topic, data))
      case Schema.Type.FLOAT =>
        val deser = Serdes.floatSerde.deserializer()
        (data: Array[Byte]) => Try(deser.deserialize(topic, data))
      case Schema.Type.DOUBLE =>
        val deser = Serdes.doubleSerde.deserializer()
        (data: Array[Byte]) => Try(deser.deserialize(topic, data))
      case Schema.Type.BYTES =>
        val deser = Serdes.byteArraySerde.deserializer()
        (data: Array[Byte]) => Try(deser.deserialize(topic, data))
      case Schema.Type.NULL =>
        (_: Array[Byte]) => Success(null)

      case _ => throw new RuntimeException(s"unsupported value schema: ${pair.value.toString}")
    }

  def toGenericRecord(ccr: KafkaByteConsumerRecord): Try[GenericData.Record] = {
    val res = for {
      key <- key_decode(ccr.key)
      value <- val_decode(ccr.value)
    } yield {
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
      record.put("serializedKeySize", ccr.serializedKeySize())
      record.put("serializedValueSize", ccr.serializedValueSize())
      record.put("key", key)
      record.put("value", value)
      record.put("headers", headers.toList.asJava)
      record.put("leaderEpoch", ccr.leaderEpoch().toScala.orNull)
      record
    }

    res match {
      case Failure(exception) => Failure(new Exception(CRMetaInfo(ccr).asJson.noSpaces, exception))
      case good @ Success(_)  => good
    }
  }

  def toGenericRecord(ccr: ConsumerRecord[Array[Byte], Array[Byte]]): Try[GenericData.Record] =
    toGenericRecord(ccr.transformInto[KafkaByteConsumerRecord])
}
