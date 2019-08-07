package com.github.chenharryhua.nanjin.kafka

import cats.Bitraverse
import cats.implicits._
import monocle.Iso

import scala.util.{Failure, Success, Try}

trait KafkaMessageConversion[K, V] {
  val keyIso: Iso[Array[Byte], K]
  val valueIso: Iso[Array[Byte], V]

  final private def option[A](a: A): Try[A] =
    Option(a).fold[Try[A]](Failure(new Exception("null object")))(Success(_))

  def decode[F[_, _]: Bitraverse](data: F[Array[Byte], Array[Byte]]): F[K, V] =
    data.bimap(keyIso.get, valueIso.get)

  def decodeKey[F[_, _]: Bitraverse](data: F[Array[Byte], Array[Byte]]): F[K, Array[Byte]] =
    data.bimap(keyIso.get, identity)

  def decodeValue[F[_, _]: Bitraverse](data: F[Array[Byte], Array[Byte]]): F[Array[Byte], V] =
    data.bimap(identity, valueIso.get)

  def safeDecodeKeyValue[F[_, _]: Bitraverse](
    data: F[Array[Byte], Array[Byte]]): F[Try[K], Try[V]] =
    data.bimap(
      k => option(k).flatMap(x => Try(keyIso.get(x))),
      v => option(v).flatMap(x => Try(valueIso.get(x))))

  def safeDecodeMessage[F[_, _]: Bitraverse](data: F[Array[Byte], Array[Byte]]): Try[F[K, V]] =
    data.bitraverse(
      k => option(k).flatMap(x => Try(keyIso.get(x))),
      v => option(v).flatMap(x => Try(valueIso.get(x))))

  def safeDecodeValue[F[_, _]: Bitraverse](
    data: F[Array[Byte], Array[Byte]]): Try[F[Array[Byte], V]] =
    data.bitraverse(Success(_), v => option(v).flatMap(x => Try(valueIso.get(x))))

  def safeDecodeKey[F[_, _]: Bitraverse](
    data: F[Array[Byte], Array[Byte]]): Try[F[K, Array[Byte]]] =
    data.bitraverse(k => option(k).flatMap(x => Try(keyIso.get(x))), Success(_))

  def encode[F[_, _]: Bitraverse](msg: F[K, V]): F[Array[Byte], Array[Byte]] =
    msg.bimap(k => keyIso.reverseGet(k), v => valueIso.reverseGet(v))

}
