package com.github.chenharryhua.nanjin.codec

import cats.implicits._
import com.sksamuel.avro4s.{Record, SchemaFor, Encoder => AvroEncoder}
import io.circe.{Json, Encoder                         => JsonEncoder}

import scala.util.{Success, Try}

final class KafkaGenericDecoder[F[_, _], K, V](
  data: F[Array[Byte], Array[Byte]],
  keyCodec: KafkaCodec.Key[K],
  valueCodec: KafkaCodec.Value[V])(implicit BM: BitraverseMessage[F]) {

  def decode: F[K, V]                = data.bimap(keyCodec.decode, valueCodec.decode)
  def decodeKey: F[K, Array[Byte]]   = data.bimap(keyCodec.decode, identity)
  def decodeValue: F[Array[Byte], V] = data.bimap(identity, valueCodec.decode)

  def tryDecodeKeyValue: F[Try[K], Try[V]]   = data.bimap(keyCodec.tryDecode, valueCodec.tryDecode)
  def tryDecode: Try[F[K, V]]                = data.bitraverse(keyCodec.tryDecode, valueCodec.tryDecode)
  def tryDecodeValue: Try[F[Array[Byte], V]] = data.bitraverse(Success(_), valueCodec.tryDecode)
  def tryDecodeKey: Try[F[K, Array[Byte]]]   = data.bitraverse(keyCodec.tryDecode, Success(_))

  def optionalDecodeKeyValue: F[Option[K], Option[V]] =
    data.bimap(k => keyCodec.tryDecode(k).toOption, v => valueCodec.tryDecode(v).toOption)

  def nullableDecode(implicit knull: Null <:< K, vnull: Null <:< V): F[K, V] =
    optionalDecodeKeyValue.bimap(_.orNull, _.orNull)

  def json(
    implicit
    jke: JsonEncoder[K],
    jkv: JsonEncoder[V]): Json = BM.jsonRecord(optionalDecodeKeyValue)

  def avro(
    implicit
    ks: SchemaFor[K],
    ke: AvroEncoder[K],
    vs: SchemaFor[V],
    ve: AvroEncoder[V]): Record = BM.avroRecord(optionalDecodeKeyValue)
}
