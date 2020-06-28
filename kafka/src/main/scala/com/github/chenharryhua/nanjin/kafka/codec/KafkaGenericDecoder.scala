package com.github.chenharryhua.nanjin.kafka.codec

import cats.implicits._
import com.github.chenharryhua.nanjin.messages.kafka._

import scala.util.{Success, Try}

final class KafkaGenericDecoder[F[_, _], K, V](
  data: F[Array[Byte], Array[Byte]],
  keyCodec: NJCodec[K],
  valCodec: NJCodec[V])(implicit BM: NJConsumerMessage[F]) {

  def decode: F[K, V]                = data.bimap(keyCodec.decode, valCodec.decode)
  def decodeKey: F[K, Array[Byte]]   = data.bimap(keyCodec.decode, identity)
  def decodeValue: F[Array[Byte], V] = data.bimap(identity, valCodec.decode)

  def tryDecodeKeyValue: F[Try[K], Try[V]]   = data.bimap(keyCodec.tryDecode, valCodec.tryDecode)
  def tryDecode: Try[F[K, V]]                = data.bitraverse(keyCodec.tryDecode, valCodec.tryDecode)
  def tryDecodeValue: Try[F[Array[Byte], V]] = data.bitraverse(Success(_), valCodec.tryDecode)
  def tryDecodeKey: Try[F[K, Array[Byte]]]   = data.bitraverse(keyCodec.tryDecode, Success(_))

  @SuppressWarnings(Array("AsInstanceOf"))
  def nullableDecode: F[K, V] =
    data.bimap(
      k => keyCodec.prism.getOption(k).getOrElse(null.asInstanceOf[K]),
      v => valCodec.prism.getOption(v).getOrElse(null.asInstanceOf[V]))
}
