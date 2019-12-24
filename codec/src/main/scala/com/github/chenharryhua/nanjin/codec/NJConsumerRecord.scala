package com.github.chenharryhua.nanjin.codec

import cats.Show
import com.github.ghik.silencer.silent
import com.sksamuel.avro4s.{Encoder => AvroEncoder, Record, SchemaFor, ToRecord}
import io.circe.generic.JsonCodec
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder => JsonDecoder, Encoder => JsonEncoder}
import monocle.Iso
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.record.TimestampType

@JsonCodec
sealed trait NJTimestampType {
  val name: String
  val id: Int
}

object NJTimestampType {

  case object NO_TIMESTAMP_TYPE extends NJTimestampType {
    override val name: String = "NO_TIMESTAMP_TYPE"
    override val id: Int      = -1
  }

  case object CREATE_TIME extends NJTimestampType {
    override val name: String = "CREATE_TIME"
    override val id: Int      = 0
  }

  case object LOG_APPEND_TIME extends NJTimestampType {
    override val name: String = "LOG_APPEND_TIME"
    override val id: Int      = 1
  }

  val iso: Iso[NJTimestampType, TimestampType] =
    Iso[NJTimestampType, TimestampType]({
      case CREATE_TIME       => TimestampType.CREATE_TIME
      case LOG_APPEND_TIME   => TimestampType.LOG_APPEND_TIME
      case NO_TIMESTAMP_TYPE => TimestampType.NO_TIMESTAMP_TYPE
    })({
      case TimestampType.CREATE_TIME       => CREATE_TIME
      case TimestampType.LOG_APPEND_TIME   => LOG_APPEND_TIME
      case TimestampType.NO_TIMESTAMP_TYPE => NO_TIMESTAMP_TYPE
    })
}

final case class NJConsumerRecord[K, V](
  key: Option[K],
  value: Option[V],
  topic: String,
  partition: Int,
  offset: Long,
  timestamp: Long,
  timestampType: NJTimestampType,
  checksum: Long,
  serializedKeySize: Int,
  serializedValueSize: Int) {

  def consumerRcord(implicit knull: Null <:< K, vnull: Null <:< V): ConsumerRecord[K, V] =
    new ConsumerRecord[K, V](
      this.topic,
      this.partition,
      this.offset,
      this.timestamp,
      NJTimestampType.iso.get(this.timestampType),
      this.checksum,
      this.serializedKeySize,
      this.serializedValueSize,
      this.key.orNull,
      this.value.orNull)

  def asAvro(
    implicit
    ks: SchemaFor[K],
    ke: AvroEncoder[K],
    vs: SchemaFor[V],
    ve: AvroEncoder[V]): Record =
    ToRecord[NJConsumerRecord[K, V]].to(this)

}

object NJConsumerRecord {

  def apply[K, V](cr: ConsumerRecord[K, V]): NJConsumerRecord[K, V] =
    NJConsumerRecord(
      Option(cr.key),
      Option(cr.value),
      cr.topic,
      cr.partition,
      cr.offset,
      cr.timestamp,
      NJTimestampType.iso.reverseGet(cr.timestampType),
      cr.checksum: @silent,
      cr.serializedKeySize,
      cr.serializedValueSize
    )

  implicit def jsonNJConsumerRecordEncoder[K: JsonEncoder, V: JsonEncoder]
    : JsonEncoder[NJConsumerRecord[K, V]] =
    deriveEncoder[NJConsumerRecord[K, V]]

  implicit def jsonNJConsumerRecordDecoder[K: JsonDecoder, V: JsonDecoder]
    : JsonDecoder[NJConsumerRecord[K, V]] =
    deriveDecoder[NJConsumerRecord[K, V]]

  implicit def showNJConsumerRecord[K: Show, V: Show](
    implicit
    knull: Null <:< K,
    vnull: Null <:< V): Show[NJConsumerRecord[K, V]] = { t: NJConsumerRecord[K, V] =>
    show.showConsumerRecord[K, V].show(t.consumerRcord)
  }
}
