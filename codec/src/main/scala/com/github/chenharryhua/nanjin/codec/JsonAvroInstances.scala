package com.github.chenharryhua.nanjin.codec

import monocle.Iso
import org.apache.kafka.clients.consumer.ConsumerRecord
import com.sksamuel.avro4s._

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
    (a: F[K, V]) => NJConsumerRecord[K, V](iso.get(a)).asJson

  implicit def jsonDecodeInGeneral[F[_, _], K: Decoder, V: Decoder](
    implicit
    knull: Null <:< K,
    vnull: Null <:< V,
    iso: Iso[F[K, V], ConsumerRecord[K, V]]): Decoder[F[K, V]] =
    (c: HCursor) => c.as[NJConsumerRecord[K, V]].map(x => iso.reverseGet(x.consumerRcord))
}

private[codec] trait LowPriorityAvroSyntax {

  implicit class GenericAvroSyntax[A: SchemaFor: Decoder: Encoder](a: A) {
    private val format: RecordFormat[A] = RecordFormat[A]
    def asAvro: Record                  = format.to(a)
  }
}

object avro extends LowPriorityAvroSyntax {

  implicit class AvroGenericRecordSyntax[
    F[_, _],
    K: SchemaFor: Decoder: Encoder,
    V: SchemaFor: Decoder: Encoder](cr: F[K, V])(implicit iso: Iso[F[K, V], ConsumerRecord[K, V]]) {
    private val format: RecordFormat[NJConsumerRecord[K, V]] = RecordFormat[NJConsumerRecord[K, V]]

    def asAvro: Record = format.to(NJConsumerRecord[K, V](iso.get(cr)))
  }
}
