package com.github.chenharryhua.nanjin.spark.kafka

import java.time.LocalDate

import cats.Show
import cats.implicits._
import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.datetime._
import com.github.chenharryhua.nanjin.messages.kafka.OptionalKV
import com.github.chenharryhua.nanjin.spark.saver._
import com.sksamuel.avro4s.{Encoder => AvroEncoder}
import frameless.TypedEncoder
import frameless.cats.implicits.rddOps
import io.circe.{Encoder => JsonEncoder}
import io.circe.generic.auto._
import kantan.csv.RowEncoder
import scalapb.GeneratedMessage

import scala.reflect.ClassTag

final class CrRddFileSaver[F[_], K, V](saver: RddFileSaver[F, OptionalKV[K, V]], params: SKParams)(
  implicit avroEncoder: AvroEncoder[OptionalKV[K, V]])
    extends Serializable {

  def avro(pathStr: String): AvroSaver[F, OptionalKV[K, V]] =
    saver.avro(pathStr)

  def avro: AvroSaver[F, OptionalKV[K, V]] =
    saver.avro(params.outPath(NJFileFormat.Avro))

  def jackson(pathStr: String): JacksonSaver[F, OptionalKV[K, V]] =
    saver.jackson(pathStr)

  def jackson: JacksonSaver[F, OptionalKV[K, V]] =
    saver.jackson(params.outPath(NJFileFormat.Jackson))

  def parquet(pathStr: String)(implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): ParquetSaver[F, OptionalKV[K, V]] =
    saver.parquet(pathStr)

  def parquet(implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): ParquetSaver[F, OptionalKV[K, V]] =
    saver.parquet(params.outPath(NJFileFormat.Parquet))

  def binAvro(pathStr: String): BinaryAvroSaver[F, OptionalKV[K, V]] =
    saver.binAvro(pathStr)

  def binAvro: BinaryAvroSaver[F, OptionalKV[K, V]] =
    saver.binAvro(params.outPath(NJFileFormat.BinaryAvro))

  def text(pathStr: String)(implicit ev: Show[OptionalKV[K, V]]): TextSaver[F, OptionalKV[K, V]] =
    saver.text(pathStr)

  def text(implicit ev: Show[OptionalKV[K, V]]): TextSaver[F, OptionalKV[K, V]] =
    saver.text(params.outPath(NJFileFormat.Text))

  def circe(pathStr: String)(implicit
    jsonKeyDecoder: JsonEncoder[K],
    jsonValDecoder: JsonEncoder[V]): CirceSaver[F, OptionalKV[K, V]] =
    saver.circe(pathStr)

  def circe(implicit
    jsonKeyDecoder: JsonEncoder[K],
    jsonValDecoder: JsonEncoder[V]): CirceSaver[F, OptionalKV[K, V]] =
    saver.circe(params.outPath(NJFileFormat.Circe))

  def javaObject(pathStr: String): JavaObjectSaver[F, OptionalKV[K, V]] =
    saver.javaObject(pathStr)

  def javaObject: JavaObjectSaver[F, OptionalKV[K, V]] =
    saver.javaObject(params.outPath(NJFileFormat.JavaObject))

  def csv[A: RowEncoder](pathStr: String)(f: OptionalKV[K, V] => A)(implicit
    ev: TypedEncoder[A]): CsvSaver[F, A] = {
    import ev.classTag
    saver.map(f).csv(pathStr)
  }

  def csv[A: RowEncoder](f: OptionalKV[K, V] => A)(implicit ev: TypedEncoder[A]): CsvSaver[F, A] =
    csv(params.outPath(NJFileFormat.Csv))(f)

  def protobuf[A: ClassTag](pathStr: String)(f: OptionalKV[K, V] => A)(implicit
    ev: A <:< GeneratedMessage): ProtobufSaver[F, A] =
    saver.map(f).protobuf(pathStr)

  def protobuf[A: ClassTag](f: OptionalKV[K, V] => A)(implicit
    ev: A <:< GeneratedMessage): ProtobufSaver[F, A] =
    protobuf(params.outPath(NJFileFormat.ProtoBuf))(f)

  def dump: Dumper[F, OptionalKV[K, V]] =
    saver.dump(params.replayPath)

  object partition extends Serializable {

    private def bucketing(kv: OptionalKV[K, V]): LocalDate =
      NJTimestamp(kv.timestamp).dayResolution(params.timeRange.zoneId)

    def jackson: JacksonPartitionSaver[F, OptionalKV[K, V], LocalDate] =
      saver.partition.jackson[LocalDate](bucketing, params.datePartition(NJFileFormat.Jackson))

    def parquet(implicit
      keyEncoder: TypedEncoder[K],
      valEncoder: TypedEncoder[V]): ParquetPartitionSaver[F, OptionalKV[K, V], LocalDate] =
      saver.partition.parquet[LocalDate](bucketing, params.datePartition(NJFileFormat.Parquet))

    def binAvro: BinaryAvroPartitionSaver[F, OptionalKV[K, V], LocalDate] =
      saver.partition.binAvro[LocalDate](bucketing, params.datePartition(NJFileFormat.BinaryAvro))

    def text(implicit
      ev: Show[OptionalKV[K, V]]): TextPartitionSaver[F, OptionalKV[K, V], LocalDate] =
      saver.partition.text[LocalDate](bucketing, params.datePartition(NJFileFormat.Text))

    def avro: AvroPartitionSaver[F, OptionalKV[K, V], LocalDate] =
      saver.partition.avro[LocalDate](bucketing, params.datePartition(NJFileFormat.Avro))

    def javaObject: JavaObjectPartitionSaver[F, OptionalKV[K, V], LocalDate] =
      saver.partition
        .javaObject[LocalDate](bucketing, params.datePartition(NJFileFormat.JavaObject))

    def circe(implicit
      jsonKeyDecoder: JsonEncoder[K],
      jsonValDecoder: JsonEncoder[V]): CircePartitionSaver[F, OptionalKV[K, V], LocalDate] =
      saver.partition.circe[LocalDate](bucketing, params.datePartition(NJFileFormat.Circe))
  }
}
