package com.github.chenharryhua.nanjin.messages.kafka

import cats.Show
import io.circe.{Decoder as JsonDecoder, Encoder as JsonEncoder}
import io.circe.generic.auto.*
import io.scalaland.chimney.dsl.*

import java.time.{Instant, ZoneId, ZonedDateTime}
import scala.annotation.nowarn

final case class NJConsumerRecordWithError[K, V](
  partition: Int,
  offset: Long,
  timestamp: Long,
  key: Either[String, K],
  value: Either[String, V],
  topic: String,
  timestampType: Int,
  headers: List[NJHeader]) {

  def metaInfo(zoneId: ZoneId): RecordMetaInfo =
    this
      .into[RecordMetaInfo]
      .withFieldComputed(_.timestamp, x => ZonedDateTime.ofInstant(Instant.ofEpochMilli(x.timestamp), zoneId))
      .transform

  def toNJConsumerRecord: NJConsumerRecord[K, V] = this
    .into[NJConsumerRecord[K, V]]
    .withFieldComputed(_.key, _.key.toOption)
    .withFieldComputed(_.value, _.value.toOption)
    .transform
}

object NJConsumerRecordWithError {
  implicit def showNJConsumerRecordWithError[K: Show, V: Show]: Show[NJConsumerRecordWithError[K, V]] =
    cats.derived.semiauto.show[NJConsumerRecordWithError[K, V]]

  implicit def jsonEncoderNJConsumerRecordWithError[K, V](implicit
    @nowarn jk: JsonEncoder[K],
    @nowarn jv: JsonEncoder[V]): JsonEncoder[NJConsumerRecordWithError[K, V]] =
    io.circe.generic.semiauto.deriveEncoder[NJConsumerRecordWithError[K, V]]

  implicit def jsonDecoderNJConsumerRecordWithError[K, V](implicit
    @nowarn jk: JsonDecoder[K],
    @nowarn jv: JsonDecoder[V]): JsonDecoder[NJConsumerRecordWithError[K, V]] =
    io.circe.generic.semiauto.deriveDecoder[NJConsumerRecordWithError[K, V]]
}
