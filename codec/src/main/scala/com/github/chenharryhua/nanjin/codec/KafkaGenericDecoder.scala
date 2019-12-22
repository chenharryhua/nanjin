package com.github.chenharryhua.nanjin.codec

import cats.Bitraverse
import cats.implicits._
import com.sksamuel.avro4s.{Record, SchemaFor, ToRecord, Encoder => AvroEncoder}
import io.circe.{Json, Encoder                                   => JsonEncoder}

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

  def nullableDecode(implicit knull: Null <:< K, vnull: Null <:< V): F[K, V] =
    data.bimap(k => keyCodec.prism.getOption(k).orNull, v => valueCodec.prism.getOption(v).orNull)

  def nullableDecodeValue(implicit vnull: Null <:< V): F[Array[Byte], V] =
    data.bimap(identity, v => valueCodec.prism.getOption(v).orNull)

  def nullableDecodeKey(implicit knull: Null <:< K): F[K, Array[Byte]] =
    data.bimap(k => keyCodec.prism.getOption(k).orNull, identity)

  def genericRecord(
    implicit
    ke: AvroEncoder[K],
    ks: SchemaFor[K],
    ve: AvroEncoder[V],
    vs: SchemaFor[V]): F[Record, Record] =
    decode.bimap(ToRecord[K].to, ToRecord[V].to)

  def genericKeyRecord(
    implicit
    ke: AvroEncoder[K],
    ks: SchemaFor[K]): F[Record, Array[Byte]] =
    decodeKey.bimap(ToRecord[K].to, identity)

  def genericValueRecord(
    implicit
    ve: AvroEncoder[V],
    vs: SchemaFor[V]): F[Array[Byte], Record] =
    decodeValue.bimap(identity, ToRecord[V].to)

  def json(implicit jk: JsonEncoder[K], jv: JsonEncoder[V]): F[Json, Json] =
    decode.bimap(jk.apply, jv.apply)

  def jsonKey(implicit jk: JsonEncoder[K]): F[Json, Array[Byte]] =
    decodeKey.bimap(jk.apply, identity)

  def jsonValue(implicit jv: JsonEncoder[V]): F[Array[Byte], Json] =
    decodeValue.bimap(identity, jv.apply)

}
