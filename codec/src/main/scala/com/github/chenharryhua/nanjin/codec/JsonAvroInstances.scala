package com.github.chenharryhua.nanjin.codec

import monocle.Iso
import org.apache.kafka.clients.consumer.ConsumerRecord

object json {
  import io.circe.generic.auto._
  import io.circe.syntax._
  import io.circe.{Decoder, Encoder, HCursor}
  import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}

  implicit def jsonNJConsumerRecordEncoder[K: Encoder, V: Encoder]
    : Encoder[NJConsumerRecord[K, V]] =
    deriveEncoder[NJConsumerRecord[K, V]]

  implicit def jsonNJConsumerRecordDecoder[K: Decoder, V: Decoder]
    : Decoder[NJConsumerRecord[K, V]] =
    deriveDecoder[NJConsumerRecord[K, V]]

  implicit def jsonEncodeInGeneral[F[_, _], K: Encoder, V: Encoder](
    implicit iso: Iso[F[K, V], ConsumerRecord[K, V]],
    knull: Null <:< K,
    vnull: Null <:< V): Encoder[F[K, V]] =
    (a: F[K, V]) => NJConsumerRecord.iso[K, V].reverseGet(iso.get(a)).asJson

  implicit def jsonDecodeInGeneral[F[_, _], K: Decoder, V: Decoder](
    implicit
    knull: Null <:< K,
    vnull: Null <:< V,
    iso: Iso[F[K, V], ConsumerRecord[K, V]]): Decoder[F[K, V]] =
    (c: HCursor) =>
      c.as[NJConsumerRecord[K, V]].map(x => iso.reverseGet(NJConsumerRecord.iso[K, V].get(x)))
}

object avro {
  import com.sksamuel.avro4s._

  implicit class AvroGenericRecordSyntax[
    K: SchemaFor: Decoder: Encoder,
    V: SchemaFor: Decoder: Encoder](cr: ConsumerRecord[K, V])(
    implicit
    knull: Null <:< K,
    vnull: Null <:< V) {
    private val format: RecordFormat[NJConsumerRecord[K, V]] = RecordFormat[NJConsumerRecord[K, V]]

    def asAvro: Record = format.to(NJConsumerRecord.iso[K, V].reverseGet(cr))
  }

  implicit class AvroGenericRecordSyntax2[
    K: SchemaFor: Decoder: Encoder,
    V: SchemaFor: Decoder: Encoder](cr: fs2.kafka.ConsumerRecord[K, V])(
    implicit
    knull: Null <:< K,
    vnull: Null <:< V) {
    def asAvro: Record = iso.isoFs2ComsumerRecord[K, V].get(cr).asAvro
  }
}
