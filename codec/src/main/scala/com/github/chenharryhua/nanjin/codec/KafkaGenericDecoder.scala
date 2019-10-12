package com.github.chenharryhua.nanjin.codec

import cats.Bitraverse
import cats.implicits._

import scala.util.{Success, Try}

final class KafkaGenericDecoder[F[_, _]: Bitraverse, K, V](
  data: F[Array[Byte], Array[Byte]],
  keyCodec: KafkaCodec.Key[K],
  valueCodec: KafkaCodec.Value[V]) {

  def decode: F[K, V]                = data.bimap(keyCodec.decode, valueCodec.decode)
  def decodeKey: F[K, Array[Byte]]   = data.bimap(keyCodec.decode, identity)
  def decodeValue: F[Array[Byte], V] = data.bimap(identity, valueCodec.decode)

  def tryDecodeKeyValue: F[Try[K], Try[V]]   = data.bimap(keyCodec.tryDecode, valueCodec.tryDecode)
  def tryDecode: Try[F[K, V]]                = data.bitraverse(keyCodec.tryDecode, valueCodec.tryDecode)
  def tryDecodeValue: Try[F[Array[Byte], V]] = data.bitraverse(Success(_), valueCodec.tryDecode)
  def tryDecodeKey: Try[F[K, Array[Byte]]]   = data.bitraverse(keyCodec.tryDecode, Success(_))

  def nullableDecode(implicit ev: Null <:< K, ev2: Null <:< V): F[K, V] =
    data.bimap(k => keyCodec.prism.getOption(k).orNull, v => valueCodec.prism.getOption(v).orNull)

  def nullableDecodeValue(implicit ev2: Null <:< V): F[Array[Byte], V] =
    data.bimap(identity, v => valueCodec.prism.getOption(v).orNull)

  def nullableDecodeKey(implicit ev: Null <:< K): F[K, Array[Byte]] =
    data.bimap(k => keyCodec.prism.getOption(k).orNull, identity)

}
