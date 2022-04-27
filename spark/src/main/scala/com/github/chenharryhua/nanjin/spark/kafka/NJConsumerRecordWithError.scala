package com.github.chenharryhua.nanjin.spark.kafka

import cats.Show
import com.github.chenharryhua.nanjin.kafka.KeyValueCodecPair
import com.github.chenharryhua.nanjin.messages.kafka.*
import io.circe.generic.JsonCodec
import io.scalaland.chimney.dsl.*
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.typelevel.cats.time.instances.instant
import java.time.Instant

@JsonCodec
final case class ConsumerRecordMetaInfo(topic: String, partition: Int, offset: Long, timestamp: Instant)
object ConsumerRecordMetaInfo extends instant {
  implicit val showConsumerRecordMetaInfo: Show[ConsumerRecordMetaInfo] =
    cats.derived.semiauto.show[ConsumerRecordMetaInfo]
}

final case class NJConsumerRecordWithError[K, V] private (
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

object NJConsumerRecordWithError {
  def apply[G[_, _], K, V](codec: KeyValueCodecPair[K, V], gaa: G[Array[Byte], Array[Byte]])(implicit
    cm: NJConsumerMessage[G]): NJConsumerRecordWithError[K, V] = {
    val cr: ConsumerRecord[Array[Byte], Array[Byte]] = cm.lens.get(gaa)
    val k: Either[Throwable, K]                      = codec.keyCodec.tryDecode(cr.key()).toEither
    val v: Either[Throwable, V]                      = codec.valCodec.tryDecode(cr.value()).toEither
    NJConsumerRecordWithError(cr.partition, cr.offset, cr.timestamp, k, v, cr.topic, cr.timestampType.id)
  }
}
