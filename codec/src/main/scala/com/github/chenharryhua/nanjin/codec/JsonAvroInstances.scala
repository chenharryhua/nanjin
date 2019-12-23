package com.github.chenharryhua.nanjin.codec

import monocle.Iso
import org.apache.kafka.clients.consumer.ConsumerRecord

object json {
  import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
  import io.circe.syntax._
  import io.circe.{Decoder, Encoder, HCursor}

  implicit def jsonNJConsumerRecordEncoder[K: Encoder, V: Encoder]
    : Encoder[NJConsumerRecord[K, V]] =
    deriveEncoder[NJConsumerRecord[K, V]]

  implicit def jsonNJConsumerRecordDecoder[K: Decoder, V: Decoder]
    : Decoder[NJConsumerRecord[K, V]] =
    deriveDecoder[NJConsumerRecord[K, V]]

  implicit def jsonEncodeInGeneral[F[_, _], K: Encoder, V: Encoder](
    implicit iso: Iso[F[K, V], ConsumerRecord[K, V]]): Encoder[F[K, V]] =
    (a: F[K, V]) => NJConsumerRecord.from[K, V](iso.get(a)).asJson

  implicit def jsonDecodeInGeneral[F[_, _], K: Decoder, V: Decoder](
    implicit
    knull: Null <:< K,
    vnull: Null <:< V,
    iso: Iso[F[K, V], ConsumerRecord[K, V]]): Decoder[F[K, V]] =
    (c: HCursor) => c.as[NJConsumerRecord[K, V]].map(x => iso.reverseGet(x.consumerRcord))
}

object avro {
  import com.sksamuel.avro4s._

  implicit class AvroGenericRecordSyntax[K: SchemaFor: Decoder: Encoder,
  V: SchemaFor: Decoder: Encoder](cr: ConsumerRecord[K, V]) {
    private val format: RecordFormat[NJConsumerRecord[K, V]] = RecordFormat[NJConsumerRecord[K, V]]

    def asAvro: Record = format.to(NJConsumerRecord.from[K, V](cr))
  }

  implicit class AvroGenericRecordSyntax2[K: SchemaFor: Decoder: Encoder,
  V: SchemaFor: Decoder: Encoder](cr: fs2.kafka.ConsumerRecord[K, V]) {
    def asAvro: Record = iso.isoFs2ComsumerRecord[K, V].get(cr).asAvro
  }
}
