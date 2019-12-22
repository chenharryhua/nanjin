package com.github.chenharryhua.nanjin.codec

import io.circe.syntax._
import io.circe.{Decoder, Encoder, HCursor, Json}
import monocle.Iso
import org.apache.kafka.clients.consumer.ConsumerRecord

private[codec] trait JsonInstances {
  private val TOPIC         = "topic"
  private val PARTITION     = "partition"
  private val OFFSET        = "offset"
  private val TIMESTAMP     = "timestamp"
  private val TIMESTAMPTYPE = "timestamp_type"
  private val KEY           = "key"
  private val VALUE         = "value"

  implicit def jsonEncodeConsumerRecord[K: Encoder, V: Encoder]: Encoder[ConsumerRecord[K, V]] =
    (cr: ConsumerRecord[K, V]) =>
      Json.obj(
        (TOPIC, Json.fromString(cr.topic)),
        (PARTITION, Json.fromInt(cr.partition)),
        (OFFSET, Json.fromLong(cr.offset)),
        (TIMESTAMP, Json.fromLong(cr.timestamp)),
        (TIMESTAMPTYPE, Json.fromString(cr.timestampType.toString)),
        (KEY, Option(cr.key).asJson),
        (VALUE, Option(cr.value).asJson)
      )

  implicit def jsonDecodeConsumerRecord[K: Decoder, V: Decoder](
    implicit
    knull: Null <:< K,
    vnull: Null <:< V): Decoder[ConsumerRecord[K, V]] =
    (c: HCursor) =>
      for {
        topic <- c.downField(TOPIC).as[String]
        partition <- c.downField(PARTITION).as[Int]
        offset <- c.downField(OFFSET).as[Long]
        key <- c.downField(KEY).as[Option[K]]
        value <- c.downField(VALUE).as[Option[V]]
      } yield new ConsumerRecord[K, V](topic, partition, offset, key.orNull, value.orNull)

  implicit def jsonEncodeInGeneral[F[_, _], K: Encoder, V: Encoder](
    implicit iso: Iso[F[K, V], ConsumerRecord[K, V]]): Encoder[F[K, V]] =
    (a: F[K, V]) => iso.get(a).asJson

  implicit def jsonDecodeInGeneral[F[_, _], K: Decoder, V: Decoder](
    implicit
    knull: Null <:< K,
    vnull: Null <:< V,
    iso: Iso[F[K, V], ConsumerRecord[K, V]]): Decoder[F[K, V]] =
    (c: HCursor) => jsonDecodeConsumerRecord[K, V].apply(c).map(r => iso.reverseGet(r))

}
