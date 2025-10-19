package com.github.chenharryhua.nanjin.kafka.connector
import com.github.chenharryhua.nanjin.kafka.AvroSchemaPair
import com.github.chenharryhua.nanjin.messages.kafka.instances.*
import com.github.chenharryhua.nanjin.messages.kafka.{MetaInfo, NJHeader}
import com.sksamuel.avro4s.Encoder
import fs2.kafka.{ConsumerRecord, KafkaByteConsumerRecord}
import io.circe.syntax.EncoderOps
import io.scalaland.chimney.dsl.TransformerOps
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumReader}
import org.apache.avro.io.DecoderFactory
import org.apache.kafka.streams.scala.serialization.Serdes

import scala.jdk.OptionConverters.RichOptional
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

final private class PullGenericRecord(pair: AvroSchemaPair) {
  private val schema: Schema = pair.consumerSchema
  private val topic: String = "place.holder"

  private val key_decode: Array[Byte] => Any =
    pair.key.rawSchema().getType match {
      case Schema.Type.RECORD =>
        val reader = new GenericDatumReader[GenericData.Record](pair.key.rawSchema())
        (data: Array[Byte]) =>
          if (data == null) null
          else { // drop 5: 1 byte magic, 4 bytes schema ID
            val decoder = DecoderFactory.get.binaryDecoder(data.drop(5), null)
            reader.read(null, decoder)
          }
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
      case Schema.Type.NULL =>
        (_: Array[Byte]) => null

      case us => throw new RuntimeException(s"unsupported key schema: ${us.toString}")
    }

  private val val_decode: Array[Byte] => Any =
    pair.value.rawSchema().getType match {
      case Schema.Type.RECORD =>
        val reader = new GenericDatumReader[GenericData.Record](pair.value.rawSchema())
        (data: Array[Byte]) =>
          if (data == null) null
          else { // drop 5: 1 byte magic, 4 bytes schema ID
            val decoder = DecoderFactory.get.binaryDecoder(data.drop(5), null)
            reader.read(null, decoder)
          }
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
      case Schema.Type.NULL =>
        (_: Array[Byte]) => null

      case us => throw new RuntimeException(s"unsupported value schema: ${us.toString}")
    }

  def toGenericRecord(ccr: KafkaByteConsumerRecord): Try[GenericData.Record] =
    try {
      val record: GenericData.Record = new GenericData.Record(schema)
      record.put("topic", ccr.topic)
      record.put("partition", ccr.partition)
      record.put("offset", ccr.offset)
      record.put("timestamp", ccr.timestamp())
      record.put("timestampType", ccr.timestampType().id)
      record.put("serializedKeySize", ccr.serializedKeySize())
      record.put("serializedValueSize", ccr.serializedValueSize())
      record.put("key", key_decode(ccr.key))
      record.put("value", val_decode(ccr.value))
      record.put("leaderEpoch", ccr.leaderEpoch().toScala.orNull)
      record.put(
        "headers",
        Encoder[Array[NJHeader]].encode(ccr.headers().toArray.map(_.transformInto[NJHeader])))
      Success(record)
    } catch {
      case NonFatal(ex) => Failure(new Exception(MetaInfo(ccr).asJson.noSpaces, ex))
    }

  def toGenericRecord(ccr: ConsumerRecord[Array[Byte], Array[Byte]]): Try[GenericData.Record] =
    toGenericRecord(ccr.transformInto[KafkaByteConsumerRecord])
}
