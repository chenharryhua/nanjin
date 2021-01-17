package com.github.chenharryhua.nanjin.messages.kafka.codec

import cats.syntax.all._
import com.github.chenharryhua.nanjin.messages.kafka._

import scala.util.{Success, Try}

final class KafkaGenericDecoder[F[_, _], K, V](
  data: F[Array[Byte], Array[Byte]],
  keyCodec: NJCodec[K],
  valCodec: NJCodec[V])(implicit BM: NJConsumerMessage[F]) {

  @inline def decode: F[K, V]                = data.bimap(keyCodec.decode, valCodec.decode)
  @inline def decodeKey: F[K, Array[Byte]]   = data.bimap(keyCodec.decode, identity)
  @inline def decodeValue: F[Array[Byte], V] = data.bimap(identity, valCodec.decode)

  @inline def tryDecodeKeyValue: F[Try[K], Try[V]]   = data.bimap(keyCodec.tryDecode, valCodec.tryDecode)
  @inline def tryDecode: Try[F[K, V]]                = data.bitraverse(keyCodec.tryDecode, valCodec.tryDecode)
  @inline def tryDecodeValue: Try[F[Array[Byte], V]] = data.bitraverse(Success(_), valCodec.tryDecode)
  @inline def tryDecodeKey: Try[F[K, Array[Byte]]]   = data.bitraverse(keyCodec.tryDecode, Success(_))

  @inline def optionalKeyValue: F[Option[K], Option[V]] = data.bimap(keyCodec.prism.getOption, valCodec.prism.getOption)
  @inline def optionalDecode: Option[F[K, V]]           = data.bitraverse(keyCodec.prism.getOption, valCodec.prism.getOption)
  @inline def optionalValue: Option[F[Array[Byte], V]]  = data.bitraverse(Option(_), valCodec.prism.getOption)
  @inline def optionalKey: Option[F[K, Array[Byte]]]    = data.bitraverse(keyCodec.prism.getOption, Option(_))

  @SuppressWarnings(Array("AsInstanceOf"))
  @inline def nullableDecode: F[K, V] =
    data.bimap(
      k => keyCodec.prism.getOption(k).getOrElse(null.asInstanceOf[K]),
      v => valCodec.prism.getOption(v).getOrElse(null.asInstanceOf[V]))
}
