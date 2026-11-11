package com.github.chenharryhua.nanjin.kafka.connector
import cats.implicits.catsSyntaxEither
import cats.syntax.eq.given
import com.github.chenharryhua.nanjin.kafka.AvroSchemaPair
import com.github.chenharryhua.nanjin.kafka.record.{MetaInfo, NJHeader, given}
import com.sksamuel.avro4s.SchemaFor
import fs2.kafka.{ConsumerRecord, KafkaByteConsumerRecord}
import io.circe.Json
import io.circe.syntax.given
import io.scalaland.chimney.dsl.transformInto
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.io.DecoderFactory

import java.nio.ByteBuffer
import scala.jdk.CollectionConverters.{IteratorHasAsScala, SeqHasAsJava}
import scala.jdk.OptionConverters.RichOptional
import scala.util.Try

/** A per-record decode failure from `PullGenericRecord`.
  *
  * @param isKey
  *   `true` if the key failed to decode, `false` if the value did.
  * @param metaInfo
  *   topic/partition/offset metadata of the offending record, for diagnostics.
  * @param cause
  *   the underlying decode failure, as thrown (Confluent wire-format validation or Avro decode). Its
  *   `getMessage` may be `null`, e.g. for some Avro/EOF exceptions.
  */
final case class PullError(isKey: Boolean, metaInfo: MetaInfo, cause: Throwable) {

  /** The failure as one JSON object keyed `key.error` or `value.error`, holding the record's metadata fields
    * plus a `cause` naming the exception class and, when it has one, its message.
    */
  def toJson: Json = {
    val reason: String =
      Option(cause.getMessage).fold(cause.getClass.getName)(msg => s"${cause.getClass.getName}: $msg")
    Json.obj(
      (if isKey then "key.error" else "value.error") ->
        metaInfo.asJson.deepMerge(Json.obj("cause" -> Json.fromString(reason))))
  }
}

/** Decodes raw Kafka byte records into Avro `GenericRecord`s, given the key/value schema `pair`.
  */
final private class PullGenericRecord(pair: AvroSchemaPair) {
  private val schema: Schema = pair.consumerSchema

  /** Build a byte-to-value decoder for one Avro schema. `null` bytes decode to `null`. The Confluent wire
    * format is validated (length >= 5 and magic byte `0x00`) before the payload after the 5-byte prefix is
    * Avro-decoded against `schema`. This holds for record and primitive schemas alike.
    */
  private def getDecoder(schema: Schema): Array[Byte] => Any = {
    val reader = GenericDatumReader(schema)
    (data: Array[Byte]) =>
      if data eq null then null
      else if data.length < 5 then
        throw new IllegalArgumentException( // scalafix:ok
          s"Record payload too short: expected at least 5 bytes (Confluent wire format) but got ${data.length}")
      else if data(0) =!= 0.toByte then
        throw new IllegalArgumentException( // scalafix:ok
          s"Invalid Confluent wire format: expected magic byte 0x00 but got 0x${String.format("%02X", data(0))}")
      else
        val decoder = DecoderFactory.get.binaryDecoder(data.drop(5), null)
        reader.read(null, decoder)
  }

  private val key_decode: Array[Byte] => Any = getDecoder(pair.key.rawSchema())
  private val val_decode: Array[Byte] => Any = getDecoder(pair.value.rawSchema())

  private val headerSchema: Schema = SchemaFor[NJHeader].schema

  /** Decode a raw byte consumer record into a `GenericRecord`, or a `Left(PullError)` if the key or value
    * fails to decode. The result carries the decoded key/value alongside the record's metadata and headers.
    */
  def toGenericRecord(ccr: KafkaByteConsumerRecord): Either[PullError, Record] =
    for {
      key <- Try(key_decode(ccr.key())).toEither.
        leftMap(ex => PullError(true, MetaInfo(ccr), ex))
      value <- Try(val_decode(ccr.value())).toEither
        .leftMap(ex => PullError(false, MetaInfo(ccr), ex))
    } yield {
      val headers: Iterator[Record] = ccr.headers().iterator().asScala.map { h =>
        val header = new Record(headerSchema)
        header.put("key", h.key())
        // Kafka allows a null header value; the schema's bytes field does not, so it becomes empty.
        header.put("value", ByteBuffer.wrap(Option(h.value()).getOrElse(Array.emptyByteArray)))
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
      record.put("key", key)
      record.put("value", value)
      record.put("leaderEpoch", ccr.leaderEpoch().toScala.map(_.intValue()).orNull)
      record.put("headers", headers.toSeq.asJava)
      record
    }

  def toGenericRecord(ccr: ConsumerRecord[Array[Byte], Array[Byte]]): Either[PullError, Record] =
    toGenericRecord(ccr.transformInto[KafkaByteConsumerRecord])
}
