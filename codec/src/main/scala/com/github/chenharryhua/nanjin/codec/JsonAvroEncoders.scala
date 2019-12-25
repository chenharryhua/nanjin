package com.github.chenharryhua.nanjin.codec

import com.sksamuel.avro4s._
import monocle.Iso
import org.apache.kafka.clients.consumer.ConsumerRecord

object json {
  import io.circe.Encoder
  import io.circe.syntax._

  implicit def jsonEncodeInGeneral[F[_, _], K: Encoder, V: Encoder](
    implicit iso: Iso[F[K, V], ConsumerRecord[K, V]]): Encoder[F[K, V]] =
    (cr: F[K, V]) => NJConsumerRecord[K, V](iso.get(cr)).asJson
}

object avro {

  implicit class AvroEncodeInGeneral[F[_, _], K: SchemaFor: Encoder, V: SchemaFor: Encoder](
    cr: F[K, V])(implicit iso: Iso[F[K, V], ConsumerRecord[K, V]]) {

    def asAvro: Record = NJConsumerRecord[K, V](iso.get(cr)).asAvro
  }
}
