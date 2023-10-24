package com.github.chenharryhua.nanjin.messages.kafka.codec

import cats.syntax.all.*
import com.github.chenharryhua.nanjin.messages.kafka.*

import scala.util.{Success, Try}

final class KafkaGenericDecoder[K, V](keySerde: KafkaSerde[K], valSerde: KafkaSerde[V]) extends Serializable {

  @inline def decode[F[_, _]: NJConsumerMessage](data: F[Array[Byte], Array[Byte]]): F[K, V] =
    data.bimap(keySerde.deserialize, valSerde.deserialize)

  @inline def decodeKey[F[_, _]: NJConsumerMessage](data: F[Array[Byte], Array[Byte]]): F[K, Array[Byte]] =
    data.bimap(keySerde.deserialize, identity)

  @inline def decodeValue[F[_, _]: NJConsumerMessage](data: F[Array[Byte], Array[Byte]]): F[Array[Byte], V] =
    data.bimap(identity, valSerde.deserialize)

  @inline def tryDecodeKeyValue[F[_, _]: NJConsumerMessage](
    data: F[Array[Byte], Array[Byte]]): F[Try[K], Try[V]] =
    data.bimap(keySerde.tryDeserialize, valSerde.tryDeserialize)

  @inline def tryDecode[F[_, _]: NJConsumerMessage](data: F[Array[Byte], Array[Byte]]): Try[F[K, V]] =
    data.bitraverse(keySerde.tryDeserialize, valSerde.tryDeserialize)

  @inline def tryDecodeValue[F[_, _]: NJConsumerMessage](
    data: F[Array[Byte], Array[Byte]]): Try[F[Array[Byte], V]] =
    data.bitraverse(Success(_), valSerde.tryDeserialize)

  @inline def tryDecodeKey[F[_, _]: NJConsumerMessage](
    data: F[Array[Byte], Array[Byte]]): Try[F[K, Array[Byte]]] =
    data.bitraverse(keySerde.tryDeserialize, Success(_))

  @inline def optionalKeyValue[F[_, _]: NJConsumerMessage](
    data: F[Array[Byte], Array[Byte]]): F[Option[K], Option[V]] =
    tryDecodeKeyValue(data).bimap(_.toOption, _.toOption)

  @SuppressWarnings(Array("AsInstanceOf"))
  @inline def nullableDecode[F[_, _]: NJConsumerMessage](data: F[Array[Byte], Array[Byte]]): F[K, V] =
    data.bimap(
      k => keySerde.tryDeserialize(k).getOrElse(null.asInstanceOf[K]),
      v => valSerde.tryDeserialize(v).getOrElse(null.asInstanceOf[V]))
}
