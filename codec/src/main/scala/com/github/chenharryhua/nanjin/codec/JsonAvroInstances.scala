package com.github.chenharryhua.nanjin.codec

import com.sksamuel.avro4s._
import monocle.Iso
import org.apache.kafka.clients.consumer.ConsumerRecord

object json {
  import io.circe.syntax._
  import io.circe.{Decoder, Encoder, HCursor}

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

  implicit class GenericAvroEncodeSyntax[A: SchemaFor: Encoder](a: A) {
    def asAvro: Record = ToRecord[A].to(a)
  }
}

object avro extends LowPriorityAvroSyntax {

  implicit class ConsumerRecordAvroEncodeSyntax[
    F[_, _],
    K: SchemaFor: Encoder,
    V: SchemaFor: Encoder](cr: F[K, V])(implicit iso: Iso[F[K, V], ConsumerRecord[K, V]]) {

    def asAvro: Record = NJConsumerRecord[K, V](iso.get(cr)).asAvro
  }
}
