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

  def json(implicit jk: JsonEncoder[K], jv: JsonEncoder[V]): F[Json, Json] =
    tryDecodeKeyValue.bimap(
      k => k.map(jk.apply).getOrElse(Json.Null),
      v => v.map(jv.apply).getOrElse(Json.Null))

  def genericRecord(
    implicit
    ke: AvroEncoder[K],
    ks: SchemaFor[K],
    ve: AvroEncoder[V],
    vs: SchemaFor[V]): F[Record, Record] =
    tryDecodeKeyValue.bimap(
      _.map(ToRecord[K].to).toOption.orNull,
      _.map(ToRecord[V].to).toOption.orNull)
}
