package com.github.chenharryhua.nanjin.spark.kafka

import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.messages.kafka.OptionalKV
import com.github.chenharryhua.nanjin.spark.saver._
import com.sksamuel.avro4s.{Decoder => AvroDecoder, Encoder => AvroEncoder}
import frameless.cats.implicits.rddOps
import io.circe.generic.auto._
import io.circe.{Encoder => JsonEncoder}
import scalapb.GeneratedMessage

import scala.reflect.ClassTag

final class CrRddFileSaver[F[_], K, V](saver: RddFileSaver[F, OptionalKV[K, V]], params: SKParams)(
  implicit
  avroEncoder: AvroEncoder[OptionalKV[K, V]],
  avroDecoder: AvroDecoder[OptionalKV[K, V]])
    extends Serializable {

  def avro(pathStr: String): ConfigFileSaver[F, OptionalKV[K, V]] =
    saver.hadoopAvro(pathStr)

  def avro: ConfigFileSaver[F, OptionalKV[K, V]] =
    saver.hadoopAvro(params.outPath(NJFileFormat.Avro))

  def jackson(pathStr: String): ConfigFileSaver[F, OptionalKV[K, V]] =
    saver.jackson(pathStr)

  def jackson: ConfigFileSaver[F, OptionalKV[K, V]] =
    saver.jackson(params.outPath(NJFileFormat.Jackson))

  def binAvro(pathStr: String): ConfigFileSaver[F, OptionalKV[K, V]] =
    saver.binAvro(pathStr)

  def binAvro: ConfigFileSaver[F, OptionalKV[K, V]] =
    saver.binAvro(params.outPath(NJFileFormat.BinaryAvro))

  def circe(pathStr: String)(implicit
    jsonKeyDecoder: JsonEncoder[K],
    jsonValDecoder: JsonEncoder[V]): ConfigFileSaver[F, OptionalKV[K, V]] =
    saver.circe(pathStr)

  def circe(implicit
    jsonKeyDecoder: JsonEncoder[K],
    jsonValDecoder: JsonEncoder[V]): ConfigFileSaver[F, OptionalKV[K, V]] =
    saver.circe(params.outPath(NJFileFormat.Circe))

  def javaObject(pathStr: String): ConfigFileSaver[F, OptionalKV[K, V]] =
    saver.javaObject(pathStr)

  def javaObject: ConfigFileSaver[F, OptionalKV[K, V]] =
    saver.javaObject(params.outPath(NJFileFormat.JavaObject))

  def protobuf[A: ClassTag](pathStr: String)(f: OptionalKV[K, V] => A)(implicit
    ev: A <:< GeneratedMessage): ConfigFileSaver[F, A] =
    saver.map(f).protobuf(pathStr)

  def protobuf[A: ClassTag](f: OptionalKV[K, V] => A)(implicit
    ev: A <:< GeneratedMessage): ConfigFileSaver[F, A] =
    protobuf(params.outPath(NJFileFormat.ProtoBuf))(f)

  def dump: Dumper[F, OptionalKV[K, V]] =
    saver.dump(params.replayPath)
}
