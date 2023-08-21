package com.github.chenharryhua.nanjin.messages.kafka.codec

import cats.syntax.all.*
import com.github.chenharryhua.nanjin.messages.kafka.*

import scala.util.{Success, Try}

final class KafkaGenericDecoder[F[_, _], K, V](
  data: F[Array[Byte], Array[Byte]],
  keySerde: KafkaSerde[K],
  valSerde: KafkaSerde[V])(implicit BM: NJConsumerMessage[F]) {

  @inline def decode: F[K, V]                = data.bimap(keySerde.deserialize, valSerde.deserialize)
  @inline def decodeKey: F[K, Array[Byte]]   = data.bimap(keySerde.deserialize, identity)
  @inline def decodeValue: F[Array[Byte], V] = data.bimap(identity, valSerde.deserialize)

  @inline def tryDecodeKeyValue: F[Try[K], Try[V]] =
    data.bimap(keySerde.tryDeserialize, valSerde.tryDeserialize)
  @inline def tryDecode: Try[F[K, V]] = data.bitraverse(keySerde.tryDeserialize, valSerde.tryDeserialize)
  @inline def tryDecodeValue: Try[F[Array[Byte], V]] = data.bitraverse(Success(_), valSerde.tryDeserialize)
  @inline def tryDecodeKey: Try[F[K, Array[Byte]]]   = data.bitraverse(keySerde.tryDeserialize, Success(_))

  @inline def optionalKeyValue: F[Option[K], Option[V]] = tryDecodeKeyValue.bimap(_.toOption, _.toOption)

  @SuppressWarnings(Array("AsInstanceOf"))
  @inline def nullableDecode: F[K, V] =
    data.bimap(
      k => keySerde.tryDeserialize(k).getOrElse(null.asInstanceOf[K]),
      v => valSerde.tryDeserialize(v).getOrElse(null.asInstanceOf[V]))
}
