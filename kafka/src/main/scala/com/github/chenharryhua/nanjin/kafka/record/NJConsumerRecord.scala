package com.github.chenharryhua.nanjin.kafka.record

import cats.Bitraverse
import cats.data.Cont
import cats.derived.derived
import cats.kernel.Eq
import cats.syntax.apply.given
import cats.syntax.eq.given
import com.github.chenharryhua.nanjin.kafka.record.ProtoConsumerRecord.ProtoConsumerRecord
import com.google.protobuf.ByteString
import com.sksamuel.avro4s.{AvroDoc, AvroName, AvroNamespace, Encoder, SchemaFor, ToRecord}
import fs2.kafka.{ConsumerRecord, Header, Headers, Timestamp}
import io.circe.{Decoder as JsonDecoder, Encoder as JsonEncoder}
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

/** An immutable, serialization-friendly view of a Kafka consumer record.
  *
  * Mirrors the fields of the Java/fs2 consumer record but makes `key` and `value` optional (Kafka permits
  * null keys and values) and models headers as `NJHeader`. It derives JSON `Encoder`/`Decoder`, `Bitraverse`
  * (over key and value), and `Eq`; an Avro `SchemaFor`/`Encoder` is available separately (the JSON codecs are
  * kept apart from Avro because the two conflict). Conversions to and from the Java client record, the fs2
  * record, Avro `GenericRecord`, and Protobuf are provided.
  */
@AvroDoc("kafka consumer record, optional Key and optional Value")
@AvroNamespace("nanjin.kafka")
@AvroName("NJConsumerRecord")
final case class NJConsumerRecord[K, V](
  @AvroDoc("kafka topic name") topic: String,
  @AvroDoc("kafka partition") partition: Int,
  @AvroDoc("kafka offset") offset: Long,
  @AvroDoc("kafka timestamp in millisecond") timestamp: Long,
  // raw timestamp-type id: 0=CREATE_TIME, 1=LOG_APPEND_TIME, else NO_TIMESTAMP_TYPE. Kept as Int (not an
  // enum) for Spark/Avro columnar compatibility; see id_to_timestamp_type in the companion.
  @AvroDoc("kafka timestamp type") timestampType: Int,
  @AvroDoc("kafka headers") headers: List[NJHeader],
  @AvroDoc("kafka leader epoch") leaderEpoch: Option[Int],
  @AvroDoc("kafka key size") serializedKeySize: Int,
  @AvroDoc("kafka value size") serializedValueSize: Int,
  @AvroDoc("kafka key") key: Option[K],
  @AvroDoc("kafka value") value: Option[V]
) derives JsonEncoder, JsonDecoder, Bitraverse, Eq { // JSON Decoder/Encoder conflict with Avro's

  /** Collapse a nested optional key and value. Available only when both `K` and `V` are themselves `Option`s;
    * `Some(None)` becomes `None`.
    */
  def flatten[K2, V2](using K <:< Option[K2], V <:< Option[V2]): NJConsumerRecord[K2, V2] =
    copy(key = key.flatten, value = value.flatten)

  /** Collapse a nested optional key (see `flatten`), leaving the value untouched. */
  def flattenKey[K2](using K <:< Option[K2]): NJConsumerRecord[K2, V] =
    copy(key = key.flatten)

  /** Collapse a nested optional value (see `flatten`), leaving the key untouched. */
  def flattenValue[V2](using V <:< Option[V2]): NJConsumerRecord[K, V2] =
    copy(value = value.flatten)

  /** Reinterpret this consumer record as a producer record, carrying `partition`, `offset`, and `timestamp`
    * over as present values. Useful for republishing a consumed record.
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

  /** Convert to the Java Kafka client's `ConsumerRecord`. A `None` key or value becomes `null`; the evidence
    * `Null <:< K`/`Null <:< V` witnesses that the types admit null.
    */
  def toJavaConsumerRecord(using Null <:< K, Null <:< V): JavaConsumerRecord[K, V] =
    this.into[JavaConsumerRecord[K, V]].transform

  /** Convert to the fs2-kafka `ConsumerRecord`. A `None` key or value becomes `null`; the evidence
    * `Null <:< K`/`Null <:< V` witnesses that the types admit null.
    */
  def toConsumerRecord(using Null <:< K, Null <:< V): ConsumerRecord[K, V] =
    this.into[ConsumerRecord[K, V]].transform

  /** Project into a `ZonedConsumerRecord`, converting the epoch-millis `timestamp` into a `ZonedDateTime` in
    * the given zone.
    */
  def zoned(zoneId: ZoneId): ZonedConsumerRecord[K, V] =
    this.into[ZonedConsumerRecord[K, V]]
      .withFieldComputed(
        _.timestamp,
        cr => ZonedDateTime.ofInstant(Instant.ofEpochMilli(cr.timestamp), zoneId))
      .transform

  /** Encode into the Protobuf representation, using `k`/`v` to serialize the key and value to `ByteString`. A
    * `None` key or value maps to an absent Protobuf field.
    */
  def toProtobuf(k: K => ByteString, v: V => ByteString): ProtoConsumerRecord =
    this
      .into[ProtoConsumerRecord]
      .withFieldComputed(_.key, _.key.map(k))
      .withFieldComputed(_.value, _.value.map(v))
      .withFieldConst(_.unknownFields, _root_.scalapb.UnknownFieldSet.empty)
      .transform

  /** Encode into an Avro `GenericRecord`, given Avro `Encoder`/`SchemaFor` for the key and value types. */
  def toGenericRecord(using Encoder[K], Encoder[V], SchemaFor[K], SchemaFor[V]): GenericRecord = {
    val schema = summon[SchemaFor[NJConsumerRecord[K, V]]].schema
    ToRecord[NJConsumerRecord[K, V]](schema).to(this)
  }
}

object NJConsumerRecord {

  /** Build an `NJConsumerRecord` from the Java Kafka client's `ConsumerRecord`. */
  def apply[K, V](cr: JavaConsumerRecord[K, V]): NJConsumerRecord[K, V] =
    cr.into[NJConsumerRecord[K, V]].transform

  /** Build an `NJConsumerRecord` from the fs2-kafka `ConsumerRecord`. */
  def apply[K, V](cr: ConsumerRecord[K, V]): NJConsumerRecord[K, V] =
    cr.into[NJConsumerRecord[K, V]].transform

  /** Build the Avro schema for `NJConsumerRecord` from the key and value schemas, without needing static
    * `SchemaFor` instances for `K` and `V`.
    */
  def schema(keySchema: Schema, valSchema: Schema): Schema = {
    class KEY
    class VAL
    given schemaForKey: SchemaFor[KEY] = SchemaFor[KEY](keySchema)
    given schemaForVal: SchemaFor[VAL] = SchemaFor[VAL](valSchema)
    SchemaFor[NJConsumerRecord[KEY, VAL]].schema
  }

  /** Single source of truth for interpreting the raw `timestampType` id: `0` is CREATE_TIME, `1` is
    * LOG_APPEND_TIME, anything else is NO_TIMESTAMP_TYPE. The id is stored as a plain `Int` (rather than an
    * enum) to stay Spark- and Avro-columnar-friendly.
    */
  private def id_to_timestamp_type(id: Int): JavaTimestampType =
    id match {
      case 0 => JavaTimestampType.CREATE_TIME
      case 1 => JavaTimestampType.LOG_APPEND_TIME
      case _ => JavaTimestampType.NO_TIMESTAMP_TYPE
    }

  /** Chimney transformer from the Java client record; null key/value become `None`. */
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

  /** Chimney transformer to the Java client record; `None` key/value become `null` via the `Null <:<`
    * evidence, and `timestampType` maps 0/1/other to CREATE_TIME/LOG_APPEND_TIME/NO_TIMESTAMP_TYPE.
    */
  given [K, V](using
    ek: Null <:< K,
    ev: Null <:< V): Transformer[NJConsumerRecord[K, V], JavaConsumerRecord[K, V]] =
    (src: NJConsumerRecord[K, V]) =>
      new JavaConsumerRecord[K, V](
        src.topic,
        src.partition,
        src.offset,
        src.timestamp,
        id_to_timestamp_type(src.timestampType),
        src.serializedKeySize,
        src.serializedValueSize,

        src.key.getOrElse(ek(null)),
        src.value.getOrElse(ev(null)),

        new RecordHeaders(src.headers.map(_.into[JavaHeader].transform).toArray),
        src.leaderEpoch.map(Integer.valueOf).toJava
      )

  /** Chimney transformer from the fs2-kafka record; a missing timestamp collapses to NO_TIMESTAMP_TYPE and a
    * missing serialized size to `NULL_SIZE`.
    */
  given [K, V]: Transformer[ConsumerRecord[K, V], NJConsumerRecord[K, V]] =
    (src: ConsumerRecord[K, V]) => {
      val (timestampType, timestamp) =
        (src.timestamp.timestampType, src.timestamp.toOption).mapN((tt, ts) => (tt.id, ts))
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

  /** Chimney transformer to the fs2-kafka record; `None` key/value become `null`, and leader epoch and
    * serialized sizes are set only when present (a `NULL_SIZE` size is treated as absent).
    */
  given [K, V](using Null <:< K, Null <:< V): Transformer[NJConsumerRecord[K, V], ConsumerRecord[K, V]] =
    (src: NJConsumerRecord[K, V]) =>
      Cont
        .pure(
          ConsumerRecord[K, V](
            topic = src.topic,
            partition = src.partition,
            offset = src.offset,
            key = src.key.getOrElse(summon[Null <:< K](null)),
            value = src.value.getOrElse(summon[Null <:< V](null))
          ).withTimestamp(id_to_timestamp_type(src.timestampType) match {
            case JavaTimestampType.CREATE_TIME       => Timestamp.createTime(src.timestamp)
            case JavaTimestampType.LOG_APPEND_TIME   => Timestamp.logAppendTime(src.timestamp)
            case JavaTimestampType.NO_TIMESTAMP_TYPE => Timestamp.unknownTime(src.timestamp)
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
