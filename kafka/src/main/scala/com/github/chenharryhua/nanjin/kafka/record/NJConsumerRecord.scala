package com.github.chenharryhua.nanjin.kafka.record

import cats.Bitraverse
import cats.data.Cont
import cats.derived.derived
import cats.kernel.Eq
import cats.syntax.eq.catsSyntaxEq
import com.fasterxml.jackson.databind.JsonNode
import com.github.chenharryhua.nanjin.kafka.record.ProtoConsumerRecord.ProtoConsumerRecord
import com.github.chenharryhua.nanjin.kafka.serdes.globalObjectMapper
import com.google.protobuf.ByteString
import com.sksamuel.avro4s.{AvroDoc, AvroName, AvroNamespace, Decoder, Encoder, SchemaFor, ToRecord}
import fs2.kafka.*
import io.circe.Codec
import io.scalaland.chimney.Transformer
import io.scalaland.chimney.dsl.into
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecord as JavaConsumerRecord
import org.apache.kafka.common.header.Header as JavaHeader
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.record.TimestampType as JavaTimestampType

import java.time.{Instant, ZoneId, ZonedDateTime}
import scala.jdk.OptionConverters.given

@AvroDoc("kafka consumer record, optional Key and optional Value")
@AvroNamespace("nanjin.kafka")
@AvroName("NJConsumerRecord")
final case class NJConsumerRecord[K, V](
  @AvroDoc("kafka topic name") topic: String,
  @AvroDoc("kafka partition") partition: Int,
  @AvroDoc("kafka offset") offset: Long,
  @AvroDoc("kafka timestamp in millisecond") timestamp: Long,
  @AvroDoc("kafka timestamp type") timestampType: Int,
  @AvroDoc("kafka headers") headers: List[NJHeader],
  @AvroDoc("kafka leader epoch") leaderEpoch: Option[Int],
  @AvroDoc("kafka key size") serializedKeySize: Int,
  @AvroDoc("kafka value size") serializedValueSize: Int,
  @AvroDoc("kafka key") key: Option[K],
  @AvroDoc("kafka value") value: Option[V]
) derives Encoder, Decoder, SchemaFor, Bitraverse, Codec.AsObject, Eq {

  /*
   * flatten
   */
  def flatten[K2, V2](using K <:< Option[K2], V <:< Option[V2]): NJConsumerRecord[K2, V2] =
    copy(key = key.flatten, value = value.flatten)
  def flattenKey[K2](using K <:< Option[K2]): NJConsumerRecord[K2, V] =
    copy(key = key.flatten)
  def flattenValue[V2](using V <:< Option[V2]): NJConsumerRecord[K, V2] =
    copy(value = value.flatten)

  /*
   * Transition
   */

  def toNJProducerRecord: NJProducerRecord[K, V] =
    NJProducerRecord[K, V](
      topic = topic,
      partition = Some(partition),
      offset = Some(offset),
      timestamp = Some(timestamp),
      headers = headers,
      key = key,
      value = value)

  def toJavaConsumerRecord(using Null <:< K, Null <:< V): JavaConsumerRecord[K, V] =
    this.into[JavaConsumerRecord[K, V]].transform
  def toConsumerRecord(using Null <:< K, Null <:< V): ConsumerRecord[K, V] =
    this.into[ConsumerRecord[K, V]].transform

  def zoned(zoneId: ZoneId): ZonedConsumerRecord[K, V] =
    this.into[ZonedConsumerRecord[K, V]]
      .withFieldComputed(
        _.timestamp,
        cr => ZonedDateTime.ofInstant(Instant.ofEpochMilli(cr.timestamp), zoneId))
      .transform

  def toProtobuf(k: K => ByteString, v: V => ByteString): ProtoConsumerRecord =
    this
      .into[ProtoConsumerRecord]
      .withFieldComputed(_.key, _.key.map(k))
      .withFieldComputed(_.value, _.value.map(v))
      .withFieldConst(_.unknownFields, _root_.scalapb.UnknownFieldSet.empty)
      .transform

  def toJsonNode(k: K => JsonNode, v: V => JsonNode): JsonNode =
    globalObjectMapper.valueToTree[JsonNode](copy(key = key.map(k), value = value.map(v)))

  def toGenericRecord(using Encoder[K], Encoder[V], SchemaFor[K], SchemaFor[V]): GenericRecord = {
    val schema = summon[SchemaFor[NJConsumerRecord[K, V]]].schema
    ToRecord[NJConsumerRecord[K, V]](schema).to(this)
  }
}

object NJConsumerRecord {

  def apply[K, V](cr: JavaConsumerRecord[K, V]): NJConsumerRecord[K, V] =
    cr.into[NJConsumerRecord[K, V]].transform

  def apply[K, V](cr: ConsumerRecord[K, V]): NJConsumerRecord[K, V] =
    cr.into[NJConsumerRecord[K, V]].transform

  def schema(keySchema: Schema, valSchema: Schema): Schema = {
    class KEY
    class VAL
    given schemaForKey: SchemaFor[KEY] = SchemaFor[KEY](keySchema)
    given schemaForVal: SchemaFor[VAL] = SchemaFor[VAL](valSchema)
    SchemaFor[NJConsumerRecord[KEY, VAL]].schema
  }

  given [K, V]: Transformer[JavaConsumerRecord[K, V], NJConsumerRecord[K, V]] =
    (src: JavaConsumerRecord[K, V]) =>
      NJConsumerRecord(
        topic = src.topic(),
        partition = src.partition(),
        offset = src.offset(),
        timestamp = src.timestamp(),
        timestampType = src.timestampType().id,
        serializedKeySize = src.serializedKeySize(),
        serializedValueSize = src.serializedValueSize(),
        key = Option(src.key()),
        value = Option(src.value()),
        headers = src.headers().toArray.map(_.into[NJHeader].transform).toList,
        leaderEpoch = src.leaderEpoch().toScala.map(_.toInt)
      )

  given [K, V](using Null <:< K, Null <:< V): Transformer[NJConsumerRecord[K, V], JavaConsumerRecord[K, V]] =
    (src: NJConsumerRecord[K, V]) =>
      new JavaConsumerRecord[K, V](
        src.topic,
        src.partition,
        src.offset,
        src.timestamp,
        src.timestampType match {
          case 0 => JavaTimestampType.CREATE_TIME
          case 1 => JavaTimestampType.LOG_APPEND_TIME
          case _ => JavaTimestampType.NO_TIMESTAMP_TYPE
        },
        src.serializedKeySize,
        src.serializedValueSize,

        src.key.orNull,
        src.value.orNull,

        new RecordHeaders(src.headers.map(_.into[JavaHeader].transform).toArray),
        src.leaderEpoch.map(Integer.valueOf).toJava
      )

  given [K, V]: Transformer[ConsumerRecord[K, V], NJConsumerRecord[K, V]] =
    (src: ConsumerRecord[K, V]) => {
      val (timestampType, timestamp) =
        src.timestamp.createTime
          .map((JavaTimestampType.CREATE_TIME.id, _))
          .orElse(src.timestamp.logAppendTime.map((JavaTimestampType.LOG_APPEND_TIME.id, _)))
          .orElse(src.timestamp.unknownTime.map((JavaTimestampType.NO_TIMESTAMP_TYPE.id, _)))
          .getOrElse((JavaTimestampType.NO_TIMESTAMP_TYPE.id, JavaConsumerRecord.NO_TIMESTAMP))

      NJConsumerRecord(
        topic = src.topic,
        partition = src.partition,
        offset = src.offset,
        timestamp = timestamp,
        timestampType = timestampType,
        serializedKeySize = src.serializedKeySize.getOrElse(JavaConsumerRecord.NULL_SIZE),
        serializedValueSize = src.serializedValueSize.getOrElse(JavaConsumerRecord.NULL_SIZE),
        key = Option(src.key),
        value = Option(src.value),
        headers = src.headers.toChain.map(_.into[NJHeader].transform).toList,
        leaderEpoch = src.leaderEpoch
      )
    }

  given [K, V](using Null <:< K, Null <:< V): Transformer[NJConsumerRecord[K, V], ConsumerRecord[K, V]] =
    (src: NJConsumerRecord[K, V]) =>
      Cont
        .pure(
          ConsumerRecord[K, V](
            topic = src.topic,
            partition = src.partition,
            offset = src.offset,
            key = src.key.orNull,
            value = src.value.orNull
          ).withTimestamp(src.timestampType match {
            case JavaTimestampType.CREATE_TIME.id =>
              Timestamp.createTime(src.timestamp)
            case JavaTimestampType.LOG_APPEND_TIME.id =>
              Timestamp.logAppendTime(src.timestamp)
            case _ =>
              Timestamp.unknownTime(src.timestamp)
          }).withHeaders(Headers.fromSeq(src.headers.map(_.into[Header].transform))))
        .map(cr => src.leaderEpoch.fold(cr)(cr.withLeaderEpoch))
        .map(cr =>
          if (src.serializedKeySize === JavaConsumerRecord.NULL_SIZE) cr
          else cr.withSerializedKeySize(src.serializedKeySize))
        .map(cr =>
          if (src.serializedValueSize === JavaConsumerRecord.NULL_SIZE) cr
          else cr.withSerializedValueSize(src.serializedValueSize))
        .eval
        .value

}
