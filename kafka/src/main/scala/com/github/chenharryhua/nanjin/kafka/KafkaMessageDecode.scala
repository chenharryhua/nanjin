package com.github.chenharryhua.nanjin.kafka

import cats.Bitraverse
import cats.implicits._
import monocle.Iso
import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.util.{Failure, Success, Try}

abstract private[kafka] class KafkaMessageDecode[F[_, _]: Bitraverse, K, V](
  keyIso: Iso[Array[Byte], K],
  valueIso: Iso[Array[Byte], V])
    extends KafkaMessageBitraverse {

  final private def option[A](a: A): Try[A] =
    Option(a).fold[Try[A]](Failure(new Exception("decoding null object")))(Success(_))

  final def decode(cr: ConsumerRecord[Array[Byte], Array[Byte]]): ConsumerRecord[K, V] =
    cr.bimap(keyIso.get, valueIso.get)

  final def decode[G[_, _]: Bitraverse](data: G[Array[Byte], Array[Byte]]): G[K, V] =
    data.bimap(keyIso.get, valueIso.get)

  final def decodeKey(
    cr: ConsumerRecord[Array[Byte], Array[Byte]]): ConsumerRecord[K, Array[Byte]] =
    cr.bimap(keyIso.get, identity)

  final def decodeKey(data: F[Array[Byte], Array[Byte]]): F[K, Array[Byte]] =
    data.bimap(keyIso.get, identity)

  final def decodeValue(
    cr: ConsumerRecord[Array[Byte], Array[Byte]]): ConsumerRecord[Array[Byte], V] =
    cr.bimap(identity, valueIso.get)

  final def decodeValue(data: F[Array[Byte], Array[Byte]]): F[Array[Byte], V] =
    data.bimap(identity, valueIso.get)

  final def safeDecodeKeyValue(
    data: ConsumerRecord[Array[Byte], Array[Byte]]): ConsumerRecord[Try[K], Try[V]] =
    data.bimap(
      k => option(k).flatMap(x => Try(keyIso.get(x))),
      v => option(v).flatMap(x => Try(valueIso.get(x))))

  final def safeDecodeKeyValue(data: F[Array[Byte], Array[Byte]]): F[Try[K], Try[V]] =
    data.bimap(
      k => option(k).flatMap(x => Try(keyIso.get(x))),
      v => option(v).flatMap(x => Try(valueIso.get(x))))

  final def safeDecode(data: ConsumerRecord[Array[Byte], Array[Byte]]): Try[ConsumerRecord[K, V]] =
    data.bitraverse(
      k => option(k).flatMap(x => Try(keyIso.get(x))),
      v => option(v).flatMap(x => Try(valueIso.get(x))))

  final def safeDecode(data: F[Array[Byte], Array[Byte]]): Try[F[K, V]] =
    data.bitraverse(
      k => option(k).flatMap(x => Try(keyIso.get(x))),
      v => option(v).flatMap(x => Try(valueIso.get(x))))

  final def safeDecodeValue(
    data: ConsumerRecord[Array[Byte], Array[Byte]]): Try[ConsumerRecord[Array[Byte], V]] =
    data.bitraverse(Success(_), v => option(v).flatMap(x => Try(valueIso.get(x))))

  final def safeDecodeValue(data: F[Array[Byte], Array[Byte]]): Try[F[Array[Byte], V]] =
    data.bitraverse(Success(_), v => option(v).flatMap(x => Try(valueIso.get(x))))

  final def safeDecodeKey(
    data: ConsumerRecord[Array[Byte], Array[Byte]]): Try[ConsumerRecord[K, Array[Byte]]] =
    data.bitraverse(k => option(k).flatMap(x => Try(keyIso.get(x))), Success(_))

  final def safeDecodeKey(data: F[Array[Byte], Array[Byte]]): Try[F[K, Array[Byte]]] =
    data.bitraverse(k => option(k).flatMap(x => Try(keyIso.get(x))), Success(_))
}
