package com.github.chenharryhua.nanjin.messages.kafka

import cats.Show
import io.circe.generic.JsonCodec
import io.scalaland.chimney.dsl.*
import org.typelevel.cats.time.instances.instant

import java.time.Instant

@JsonCodec
final case class ConsumerRecordMetaInfo(topic: String, partition: Int, offset: Long, timestamp: Instant)
object ConsumerRecordMetaInfo extends instant {
  implicit val showConsumerRecordMetaInfo: Show[ConsumerRecordMetaInfo] =
    cats.derived.semiauto.show[ConsumerRecordMetaInfo]
}

final case class NJConsumerRecordWithError[K, V](
  partition: Int,
  offset: Long,
  timestamp: Long,
  key: Either[Throwable, K],
  value: Either[Throwable, V],
  topic: String,
  timestampType: Int) {

  def metaInfo: ConsumerRecordMetaInfo =
    this.into[ConsumerRecordMetaInfo].withFieldComputed(_.timestamp, x => Instant.ofEpochMilli(x.timestamp)).transform

  def toNJConsumerRecord: NJConsumerRecord[K, V] = this
    .into[NJConsumerRecord[K, V]]
    .withFieldComputed(_.key, _.key.toOption)
    .withFieldComputed(_.value, _.value.toOption)
    .transform
}
