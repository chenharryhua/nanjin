package com.github.chenharryhua.nanjin.messages.kafka

import cats.Bifunctor
import cats.data.Cont
import cats.kernel.Eq
import cats.syntax.eq.catsSyntaxEq
import cats.syntax.semigroup.catsSyntaxSemigroup
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.sksamuel.avro4s.*
import fs2.kafka.*
import io.circe.syntax.EncoderOps
import io.circe.{Codec as JsonCodec, Decoder as JsonDecoder, Encoder as JsonEncoder, Json}
import io.scalaland.chimney.Transformer
import io.scalaland.chimney.dsl.*
import monocle.macros.PLenses
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecord as JavaConsumerRecord
import org.apache.kafka.common.header.Header as JavaHeader
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.record.TimestampType as JavaTimestampType

import java.time.{Instant, ZoneId}
import scala.jdk.OptionConverters.{RichOption, RichOptional}

@AvroDoc("kafka consumer record, optional Key and optional Value")
@AvroNamespace("nanjin.kafka")
@AvroName("NJConsumerRecord")
@PLenses
final case class NJConsumerRecord[K, V](
  @AvroDoc("kafka topic name") topic: String,
  @AvroDoc("kafka partition") partition: Int,
  @AvroDoc("kafka offset") offset: Long,
  @AvroDoc("kafka timestamp in millisecond") timestamp: Long,
  @AvroDoc("kafka timestamp type") timestampType: Int,
  @AvroDoc("kafka headers") headers: List[NJHeader],
  @AvroDoc("kafka leader epoch") leaderEpoch: Option[Int],
  @AvroDoc("kafka key size") serializedKeySize: Option[Int],
  @AvroDoc("kafka value size") serializedValueSize: Option[Int],
  @AvroDoc("kafka key") key: Option[K],
  @AvroDoc("kafka value") value: Option[V]
) {

  def size: Option[Int] = serializedKeySize |+| serializedValueSize

  def flatten[K2, V2](implicit
    evK: K <:< Option[K2],
    evV: V <:< Option[V2]
  ): NJConsumerRecord[K2, V2] =
    copy(key = key.flatten, value = value.flatten)

  def bimap[K1, V1](k: K => K1, v: V => V1): NJConsumerRecord[K1, V1] =
    NJConsumerRecord.bifunctorNJConsumerRecord.bimap(this)(k, v)

  def toNJProducerRecord: NJProducerRecord[K, V] =
    NJProducerRecord[K, V](
      topic = topic,
      partition = Some(partition),
      offset = Some(offset),
      timestamp = Some(timestamp),
      headers = headers,
      key = key,
      value = value)

  def toJavaConsumerRecord: JavaConsumerRecord[K, V] = this.transformInto[JavaConsumerRecord[K, V]]
  def toConsumerRecord: ConsumerRecord[K, V] = this.transformInto[ConsumerRecord[K, V]]

  def zonedJson(zoneID: ZoneId)(implicit K: JsonEncoder[K], V: JsonEncoder[V]): Json =
    NJConsumerRecord
      .encoderNJConsumerRecord[K, V]
      .apply(this)
      .deepMerge(Json.obj("ts" -> Instant.ofEpochMilli(timestamp).atZone(zoneID).toLocalDateTime.asJson))
}

object NJConsumerRecord {

  def apply[K, V](cr: JavaConsumerRecord[K, V]): NJConsumerRecord[K, V] =
    cr.transformInto[NJConsumerRecord[K, V]]

  def apply[K, V](cr: ConsumerRecord[K, V]): NJConsumerRecord[K, V] =
    cr.transformInto[NJConsumerRecord[K, V]]

  def avroCodec[K, V](keyCodec: AvroCodec[K], valCodec: AvroCodec[V]): AvroCodec[NJConsumerRecord[K, V]] = {
    implicit val schemaForKey: SchemaFor[K] = keyCodec.schemaFor
    implicit val schemaForVal: SchemaFor[V] = valCodec.schemaFor
    implicit val keyDecoder: Decoder[K] = keyCodec
    implicit val valDecoder: Decoder[V] = valCodec
    implicit val keyEncoder: Encoder[K] = keyCodec
    implicit val valEncoder: Encoder[V] = valCodec
    val s: SchemaFor[NJConsumerRecord[K, V]] = implicitly
    val d: Decoder[NJConsumerRecord[K, V]] = implicitly
    val e: Encoder[NJConsumerRecord[K, V]] = implicitly
    AvroCodec[NJConsumerRecord[K, V]](s, d.withSchema(s), e.withSchema(s))
  }

  def schema(keySchema: Schema, valSchema: Schema): Schema = {
    class KEY
    class VAL
    implicit val schemaForKey: SchemaFor[KEY] = new SchemaFor[KEY] {
      override def schema: Schema = keySchema
      override def fieldMapper: FieldMapper = DefaultFieldMapper
    }

    implicit val schemaForVal: SchemaFor[VAL] = new SchemaFor[VAL] {
      override def schema: Schema = valSchema
      override def fieldMapper: FieldMapper = DefaultFieldMapper
    }
    SchemaFor[NJConsumerRecord[KEY, VAL]].schema
  }

  implicit val jsonEncoderGenericRecord: JsonEncoder[GenericRecord] =
    (a: GenericRecord) =>
        io.circe.jawn.parse(a.toString) match {
          case Left(value)  => throw value
          case Right(value) => value
        }

  implicit def encoderNJConsumerRecord[K: JsonEncoder, V: JsonEncoder]: JsonEncoder[NJConsumerRecord[K, V]] =
    io.circe.generic.semiauto.deriveEncoder[NJConsumerRecord[K, V]]

  implicit def decoderNJConsumerRecord[K: JsonDecoder, V: JsonDecoder]: JsonDecoder[NJConsumerRecord[K, V]] =
    io.circe.generic.semiauto.deriveDecoder[NJConsumerRecord[K, V]]

  def jsonCodec[K: JsonEncoder: JsonDecoder, V: JsonEncoder: JsonDecoder]: JsonCodec[NJConsumerRecord[K, V]] =
    JsonCodec.implied[NJConsumerRecord[K, V]]

  implicit val bifunctorNJConsumerRecord: Bifunctor[NJConsumerRecord] =
    new Bifunctor[NJConsumerRecord] {

      override def bimap[A, B, C, D](
        fab: NJConsumerRecord[A, B])(f: A => C, g: B => D): NJConsumerRecord[C, D] =
        fab.copy(key = fab.key.map(f), value = fab.value.map(g))
    }

  implicit def eqNJConsumerRecord[K: Eq, V: Eq]: Eq[NJConsumerRecord[K, V]] =
    Eq.instance { case (l, r) =>
      l.topic === r.topic &&
      l.partition === r.partition &&
      l.offset === r.offset &&
      l.timestamp === r.timestamp &&
      l.timestampType === r.timestampType &&
      l.serializedKeySize === r.serializedKeySize &&
      l.serializedValueSize === r.serializedValueSize &&
      l.key === r.key &&
      l.value === r.value &&
      l.headers === r.headers &&
      l.leaderEpoch === r.leaderEpoch
    }

  implicit def transformCRJavaNJ[K, V]: Transformer[JavaConsumerRecord[K, V], NJConsumerRecord[K, V]] =
    (src: JavaConsumerRecord[K, V]) =>
      NJConsumerRecord(
        topic = src.topic(),
        partition = src.partition(),
        offset = src.offset(),
        timestamp = src.timestamp(),
        timestampType = src.timestampType().id,
        serializedKeySize =
          if (src.serializedKeySize() === JavaConsumerRecord.NULL_SIZE) None
          else Some(src.serializedKeySize()),
        serializedValueSize =
          if (src.serializedValueSize() === JavaConsumerRecord.NULL_SIZE) None
          else Some(src.serializedValueSize()),
        key = Option(src.key()),
        value = Option(src.value()),
        headers = src.headers().toArray.map(_.transformInto[NJHeader]).toList,
        leaderEpoch = src.leaderEpoch().toScala.map(_.toInt)
      )

  implicit def transformCRNJJava[K, V]: Transformer[NJConsumerRecord[K, V], JavaConsumerRecord[K, V]] =
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
        src.serializedKeySize.getOrElse(JavaConsumerRecord.NULL_SIZE),
        src.serializedValueSize.getOrElse(JavaConsumerRecord.NULL_SIZE),
        src.key.getOrElse(null.asInstanceOf[K]),
        src.value.getOrElse(null.asInstanceOf[V]),
        new RecordHeaders(src.headers.map(_.transformInto[JavaHeader]).toArray),
        src.leaderEpoch.map(Integer.valueOf).toJava
      )

  implicit def transformCRFs2NJ[K, V]: Transformer[ConsumerRecord[K, V], NJConsumerRecord[K, V]] =
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
        serializedKeySize = src.serializedKeySize,
        serializedValueSize = src.serializedValueSize,
        key = Option(src.key),
        value = Option(src.value),
        headers = src.headers.toChain.map(_.transformInto[NJHeader]).toList,
        leaderEpoch = src.leaderEpoch
      )
    }

  implicit def transformCRNJFs2[K, V]: Transformer[NJConsumerRecord[K, V], ConsumerRecord[K, V]] =
    (src: NJConsumerRecord[K, V]) =>
      Cont
        .pure(ConsumerRecord[K, V](
          topic = src.topic,
          partition = src.partition,
          offset = src.offset,
          key = src.key.getOrElse(null.asInstanceOf[K]),
          value = src.value.getOrElse(null.asInstanceOf[V])
        ).withTimestamp(src.timestampType match {
          case JavaTimestampType.CREATE_TIME.id =>
            Timestamp.createTime(src.timestamp)
          case JavaTimestampType.LOG_APPEND_TIME.id =>
            Timestamp.logAppendTime(src.timestamp)
          case _ =>
            Timestamp.unknownTime(src.timestamp)
        }).withHeaders(Headers.fromSeq(src.headers.map(_.transformInto[Header]))))
        .map(cr => src.serializedKeySize.fold(cr)(cr.withSerializedKeySize))
        .map(cr => src.serializedValueSize.fold(cr)(cr.withSerializedValueSize))
        .map(cr => src.leaderEpoch.fold(cr)(cr.withLeaderEpoch))
        .eval
        .value

}
