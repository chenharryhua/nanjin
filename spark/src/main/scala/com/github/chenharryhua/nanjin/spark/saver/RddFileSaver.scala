package com.github.chenharryhua.nanjin.spark.saver

import cats.{Eq, Show}
import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.sksamuel.avro4s.{Encoder => AvroEncoder}
import frameless.TypedEncoder
import io.circe.{Encoder => JsonEncoder}
import kantan.csv.{CsvConfiguration, RowEncoder}
import org.apache.spark.rdd.RDD
import scalapb.GeneratedMessage

import scala.reflect.ClassTag

final class RddFileSaver[F[_], A](rdd: RDD[A]) extends Serializable {

  def map[B: ClassTag](f: A => B): RddFileSaver[F, B] =
    new RddFileSaver[F, B](rdd.map(f))

  def flatMap[B: ClassTag](f: A => TraversableOnce[B]): RddFileSaver[F, B] =
    new RddFileSaver[F, B](rdd.flatMap(f))

// 1
  def avro(pathStr: String)(implicit enc: AvroEncoder[A]): AvroSaver[F, A] =
    new AvroSaver[F, A](rdd, enc, pathStr, SaverConfig(NJFileFormat.Avro))

// 2
  def jackson(pathStr: String)(implicit enc: AvroEncoder[A]): JacksonSaver[F, A] =
    new JacksonSaver[F, A](rdd, enc, pathStr, SaverConfig(NJFileFormat.Jackson))

// 3
  def binAvro(pathStr: String)(implicit enc: AvroEncoder[A]): BinaryAvroSaver[F, A] =
    new BinaryAvroSaver[F, A](rdd, enc, pathStr, SaverConfig(NJFileFormat.BinaryAvro).withSingle)

// 4
  def parquet(pathStr: String)(implicit
    enc: AvroEncoder[A],
    constraint: TypedEncoder[A]): ParquetSaver[F, A] =
    new ParquetSaver[F, A](rdd, enc, constraint, pathStr, SaverConfig(NJFileFormat.Parquet))

// 5
  def circe(pathStr: String)(implicit enc: JsonEncoder[A]): CirceSaver[F, A] =
    new CirceSaver[F, A](rdd, enc, pathStr, SaverConfig(NJFileFormat.Circe))

// 6
  def text(pathStr: String)(implicit enc: Show[A]): TextSaver[F, A] =
    new TextSaver[F, A](rdd, enc, pathStr, SaverConfig(NJFileFormat.Text))

// 7
  def csv(pathStr: String)(implicit
    enc: RowEncoder[A],
    avroEncoder: AvroEncoder[A],
    constraint: TypedEncoder[A]): CsvSaver[F, A] =
    new CsvSaver[F, A](
      rdd,
      enc,
      avroEncoder,
      CsvConfiguration.rfc,
      constraint,
      pathStr,
      SaverConfig(NJFileFormat.Csv))

// 8
  def javaObject(pathStr: String): JavaObjectSaver[F, A] =
    new JavaObjectSaver[F, A](rdd, pathStr, SaverConfig(NJFileFormat.JavaObject).withSingle)

// 9
  def protobuf(pathStr: String)(implicit ev: A <:< GeneratedMessage): ProtobufSaver[F, A] =
    new ProtobufSaver[F, A](rdd, pathStr, SaverConfig(NJFileFormat.ProtoBuf).withSingle)

// 10
  def dump(pathStr: String): Dumper[F, A] =
    new Dumper[F, A](rdd, pathStr)

  object partition extends Serializable {

// 1
    def avro[K: ClassTag: Eq](bucketing: A => Option[K], pathBuilder: K => String)(implicit
      enc: AvroEncoder[A]): AvroPartitionSaver[F, A, K] =
      new AvroPartitionSaver[F, A, K](
        rdd,
        enc,
        bucketing,
        pathBuilder,
        SaverConfig(NJFileFormat.Avro))

// 2
    def jackson[K: ClassTag: Eq](bucketing: A => Option[K], pathBuilder: K => String)(implicit
      enc: AvroEncoder[A]): JacksonPartitionSaver[F, A, K] =
      new JacksonPartitionSaver[F, A, K](
        rdd,
        enc,
        bucketing,
        pathBuilder,
        SaverConfig(NJFileFormat.Jackson))

// 3
    def binAvro[K: ClassTag: Eq](bucketing: A => Option[K], pathBuilder: K => String)(implicit
      enc: AvroEncoder[A]): BinaryAvroPartitionSaver[F, A, K] =
      new BinaryAvroPartitionSaver[F, A, K](
        rdd,
        enc,
        bucketing,
        pathBuilder,
        SaverConfig(NJFileFormat.BinaryAvro).withSingle)

// 4
    def parquet[K: ClassTag: Eq](bucketing: A => Option[K], pathBuilder: K => String)(implicit
      enc: AvroEncoder[A],
      constraint: TypedEncoder[A]): ParquetPartitionSaver[F, A, K] =
      new ParquetPartitionSaver[F, A, K](
        rdd,
        enc,
        constraint,
        bucketing,
        pathBuilder,
        SaverConfig(NJFileFormat.Parquet))

// 5
    def circe[K: ClassTag: Eq](bucketing: A => Option[K], pathBuilder: K => String)(implicit
      enc: JsonEncoder[A]): CircePartitionSaver[F, A, K] =
      new CircePartitionSaver[F, A, K](
        rdd,
        enc,
        bucketing,
        pathBuilder,
        SaverConfig(NJFileFormat.Circe))

// 6
    def text[K: ClassTag: Eq](bucketing: A => Option[K], pathBuilder: K => String)(implicit
      enc: Show[A]): TextPartitionSaver[F, A, K] =
      new TextPartitionSaver[F, A, K](
        rdd,
        enc,
        bucketing,
        pathBuilder,
        SaverConfig(NJFileFormat.Text))

// 7
    def csv[K: ClassTag: Eq](bucketing: A => Option[K], pathBuilder: K => String)(implicit
      enc: RowEncoder[A],
      avroEncoder: AvroEncoder[A],
      constraint: TypedEncoder[A]): CsvPartitionSaver[F, A, K] =
      new CsvPartitionSaver[F, A, K](
        rdd,
        enc,
        avroEncoder,
        CsvConfiguration.rfc,
        constraint,
        bucketing,
        pathBuilder,
        SaverConfig(NJFileFormat.Csv))

// 8
    def javaObject[K: ClassTag: Eq](
      bucketing: A => Option[K],
      pathBuilder: K => String): JavaObjectPartitionSaver[F, A, K] =
      new JavaObjectPartitionSaver[F, A, K](
        rdd,
        bucketing,
        pathBuilder,
        SaverConfig(NJFileFormat.JavaObject).withSingle)

// 9
    def protobuf[K: ClassTag: Eq](bucketing: A => Option[K], pathBuilder: K => String)(implicit
      ev: A <:< GeneratedMessage): ProtobufPartitionSaver[F, A, K] =
      new ProtobufPartitionSaver[F, A, K](
        rdd,
        bucketing,
        pathBuilder,
        SaverConfig(NJFileFormat.ProtoBuf).withSingle)

  }
}
