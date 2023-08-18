package com.github.chenharryhua.nanjin.messages.kafka

import cats.Show
import io.scalaland.chimney.dsl.*

import java.time.{Instant, ZoneId, ZonedDateTime}

final case class NJConsumerRecordWithError[K, V](
  partition: Int,
  offset: Long,
  timestamp: Long,
  key: Either[String, K],
  value: Either[String, V],
  topic: String,
  timestampType: Int,
  headers: List[Header]) {

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
}
