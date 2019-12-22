package com.github.chenharryhua.nanjin.codec

import cats.implicits._
import com.sksamuel.avro4s.{Record, SchemaFor, ToRecord, Encoder => AvroEncoder}
import io.circe.{Json, Encoder                                   => JsonEncoder}

import scala.util.Try

private[codec] trait JsonConverter[F[_, _], K, V] { self: KafkaGenericDecoder[F, K, V] =>

  def json(implicit jk: JsonEncoder[K], jv: JsonEncoder[V]): F[Json, Json] =
    decode.bimap(jk.apply, jv.apply)

  def jsonKey(implicit jk: JsonEncoder[K]): F[Json, Array[Byte]] =
    decodeKey.bimap(jk.apply, identity)

  def jsonValue(implicit jv: JsonEncoder[V]): F[Array[Byte], Json] =
    decodeValue.bimap(identity, jv.apply)

  def tryJson(implicit jk: JsonEncoder[K], jv: JsonEncoder[V]): Try[F[Json, Json]] =
    tryDecode.map(_.bimap(jk.apply, jv.apply))

  def tryJsonKeyValue(implicit jk: JsonEncoder[K], jv: JsonEncoder[V]): F[Try[Json], Try[Json]] =
    tryDecodeKeyValue.bimap(_.map(jk.apply), _.map(jv.apply))

  def tryJsonKey(implicit jk: JsonEncoder[K]): Try[F[Json, Array[Byte]]] =
    tryDecodeKey.map(_.bimap(jk.apply, identity))

  def tryJsonValue(implicit jv: JsonEncoder[V]): Try[F[Array[Byte], Json]] =
    tryDecodeValue.map(_.bimap(identity, jv.apply))
}

private[codec] trait RecordConverter[F[_, _], K, V] { self: KafkaGenericDecoder[F, K, V] =>

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

  def tryGenericRecordKeyValue(
    implicit
    ke: AvroEncoder[K],
    ks: SchemaFor[K],
    ve: AvroEncoder[V],
    vs: SchemaFor[V]): F[Try[Record], Try[Record]] =
    tryDecodeKeyValue.bimap(_.map(ToRecord[K].to), _.map(ToRecord[V].to))

  def tryGenericRecord(
    implicit
    ke: AvroEncoder[K],
    ks: SchemaFor[K],
    ve: AvroEncoder[V],
    vs: SchemaFor[V]): Try[F[Record, Record]] =
    tryDecode.map(_.bimap(ToRecord[K].to, ToRecord[V].to))

  def tryGenericKeyRecord(
    implicit
    ke: AvroEncoder[K],
    ks: SchemaFor[K]): Try[F[Record, Array[Byte]]] =
    tryDecodeKey.map(_.bimap(ToRecord[K].to, identity))

  def tryGenericValueRecord(
    implicit
    ve: AvroEncoder[V],
    vs: SchemaFor[V]): Try[F[Array[Byte], Record]] =
    tryDecodeValue.map(_.bimap(identity, ToRecord[V].to))

}
