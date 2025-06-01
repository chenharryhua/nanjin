package com.github.chenharryhua.nanjin.kafka

import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.messages.kafka.NJHeader
import com.github.chenharryhua.nanjin.messages.kafka.codec.immigrate
import com.github.chenharryhua.nanjin.messages.kafka.instances.*
import com.sksamuel.avro4s.SchemaFor
import fs2.kafka.{ConsumerRecord, KafkaByteConsumerRecord}
import io.confluent.kafka.streams.serdes.avro.GenericAvroDeserializer
import io.scalaland.chimney.dsl.TransformerOps
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.kafka.streams.scala.serialization.Serdes

import java.nio.ByteBuffer
import scala.jdk.CollectionConverters.{MapHasAsJava, SeqHasAsJava}
import scala.jdk.OptionConverters.RichOptional
import scala.util.Try

final class PullGenericRecord(srs: SchemaRegistrySettings, topicName: TopicName, pair: AvroSchemaPair)
    extends Serializable {
  private val schema: Schema = pair.consumerSchema
  private val topic: String  = topicName.value

  @transient private lazy val key_decode: Array[Byte] => Try[Any] =
    pair.key.getType match {
      case Schema.Type.RECORD =>
        val deser = new GenericAvroDeserializer()
        deser.configure(srs.config.asJava, true)
        (data: Array[Byte]) => immigrate(pair.key, deser.deserialize(topic, data))
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

      case _ => throw new RuntimeException(s"unsupported key schema: ${pair.key.toString}")
    }

  @transient private lazy val val_decode: Array[Byte] => Try[Any] =
    pair.value.getType match {
      case Schema.Type.RECORD =>
        val deser = new GenericAvroDeserializer()
        deser.configure(srs.config.asJava, false)
        (data: Array[Byte]) => immigrate(pair.value, deser.deserialize(topic, data))
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

      case _ => throw new RuntimeException(s"unsupported value schema: ${pair.value.toString}")
    }

  def toGenericRecord(ccr: KafkaByteConsumerRecord): Try[GenericData.Record] =
    for {
      key <- key_decode(ccr.key)
      value <- val_decode(ccr.value)
    } yield {
      val record: GenericData.Record         = new GenericData.Record(schema)
      val headers: Array[GenericData.Record] = ccr.headers().toArray.map { h =>
        val header = new GenericData.Record(SchemaFor[NJHeader].schema)
        header.put("key", h.key())
        header.put("value", ByteBuffer.wrap(h.value()))
        header
      }
      record.put(TOPIC, ccr.topic)
      record.put(PARTITION, ccr.partition)
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

  def toGenericRecord(ccr: ConsumerRecord[Array[Byte], Array[Byte]]): Try[GenericData.Record] =
    toGenericRecord(ccr.transformInto[KafkaByteConsumerRecord])
}
