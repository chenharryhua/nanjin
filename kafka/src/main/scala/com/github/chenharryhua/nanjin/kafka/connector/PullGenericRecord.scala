package com.github.chenharryhua.nanjin.kafka.connector
import com.github.chenharryhua.nanjin.kafka.AvroSchemaPair
import com.github.chenharryhua.nanjin.kafka.record.{MetaInfo, NJHeader, given}
import com.sksamuel.avro4s.SchemaFor
import fs2.kafka.{ConsumerRecord, KafkaByteConsumerRecord}
import io.scalaland.chimney.dsl.transformInto
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.io.DecoderFactory
import org.apache.kafka.common.serialization.Serdes

import java.nio.ByteBuffer
import scala.jdk.CollectionConverters.SeqHasAsJava
import scala.jdk.OptionConverters.RichOptional
import scala.util.control.NonFatal

final case class PullError(metaInfo: MetaInfo, throwable: Throwable)

final private class PullGenericRecord(pair: AvroSchemaPair) {
  private val schema: Schema = pair.consumerSchema
  private val topic: String = "place.holder"

  private def getDecoder(skm: Schema): Array[Byte] => Any =
    skm.getType match {
      case Schema.Type.RECORD =>
        val reader = new GenericDatumReader[Record](skm)
        (data: Array[Byte]) =>
          if data eq null then null
          else
            // Confluent wire format: 1-byte magic + 4-byte schema ID prefix, then skip first 5 bytes
            val decoder = DecoderFactory.get.binaryDecoder(data.drop(5), null)
            reader.read(null, decoder)

      case Schema.Type.STRING =>
        val deSer = Serdes.String().deserializer()
        (data: Array[Byte]) => deSer.deserialize(topic, data)
      case Schema.Type.BYTES =>
        val deSer = Serdes.ByteArray().deserializer()
        (data: Array[Byte]) => deSer.deserialize(topic, data)
      case Schema.Type.INT =>
        val deSer = Serdes.Integer().deserializer()
        (data: Array[Byte]) => deSer.deserialize(topic, data)
      case Schema.Type.LONG =>
        val deSer = Serdes.Long().deserializer()
        (data: Array[Byte]) => deSer.deserialize(topic, data)
      case Schema.Type.FLOAT =>
        val deSer = Serdes.Float().deserializer()
        (data: Array[Byte]) => deSer.deserialize(topic, data)
      case Schema.Type.DOUBLE =>
        val deSer = Serdes.Double().deserializer()
        (data: Array[Byte]) => deSer.deserialize(topic, data)
      case Schema.Type.BOOLEAN =>
        val deSer = Serdes.Boolean().deserializer()
        (data: Array[Byte]) => deSer.deserialize(topic, data)
      case Schema.Type.NULL =>
        val deSer = Serdes.Void().deserializer()
        (data: Array[Byte]) => deSer.deserialize(topic, data)
      case us => sys.error(s"unsupported schema: ${us.toString}")
    }

  private val key_decode: Array[Byte] => Any = getDecoder(pair.key.rawSchema())
  private val val_decode: Array[Byte] => Any = getDecoder(pair.value.rawSchema())

  private val headerSchema = SchemaFor[NJHeader].schema
  def toGenericRecord(ccr: KafkaByteConsumerRecord): Either[PullError, Record] =
    try {
      val headers: Array[Record] = ccr.headers().toArray.map { h =>
        val header = new Record(headerSchema)
        header.put("key", h.key())
        header.put("value", ByteBuffer.wrap(h.value()))
        header
      }
      val record: Record = new Record(schema)
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
      record.put("headers", headers.toSeq.asJava)
      Right(record)
    } catch {
      case NonFatal(ex) => Left(PullError(MetaInfo(ccr), ex))
    }

  def toGenericRecord(ccr: ConsumerRecord[Array[Byte], Array[Byte]]): Either[PullError, Record] =
    toGenericRecord(ccr.transformInto[KafkaByteConsumerRecord])
}
