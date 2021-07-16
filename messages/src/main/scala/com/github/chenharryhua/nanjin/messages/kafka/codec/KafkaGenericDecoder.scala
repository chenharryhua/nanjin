package com.github.chenharryhua.nanjin.messages.kafka.codec

import cats.syntax.all.*
import com.github.chenharryhua.nanjin.messages.kafka.*

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

  @inline def optionalKeyValue: F[Option[K], Option[V]] = tryDecodeKeyValue.bimap(_.toOption, _.toOption)

  @SuppressWarnings(Array("AsInstanceOf"))
  @inline def nullableDecode: F[K, V] =
    data.bimap(
      k => keyCodec.tryDecode(k).getOrElse(null.asInstanceOf[K]),
      v => valCodec.tryDecode(v).getOrElse(null.asInstanceOf[V]))
}
