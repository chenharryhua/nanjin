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
    new AvroSaver[F, A](rdd, enc, SaverConfig(pathStr, NJFileFormat.Avro))

  def avro[K: ClassTag: Eq](bucketing: A => K, pathBuilder: K => String)(implicit
    enc: AvroEncoder[A]): AvroPartitionSaver[F, A, K] =
    new AvroPartitionSaver[F, A, K](
      rdd,
      enc,
      SaverConfig("", NJFileFormat.Avro),
      bucketing,
      pathBuilder)

// 2
  def jackson(pathStr: String)(implicit enc: AvroEncoder[A]): JacksonSaver[F, A] =
    new JacksonSaver[F, A](rdd, enc, SaverConfig(pathStr, NJFileFormat.Jackson))

  def jackson[K: ClassTag: Eq](bucketing: A => K, pathBuilder: K => String)(implicit
    enc: AvroEncoder[A]): JacksonPartitionSaver[F, A, K] =
    new JacksonPartitionSaver[F, A, K](
      rdd,
      enc,
      SaverConfig("", NJFileFormat.Avro),
      bucketing,
      pathBuilder)

// 3
  def binAvro(pathStr: String)(implicit enc: AvroEncoder[A]): BinaryAvroSaver[F, A] =
    new BinaryAvroSaver[F, A](rdd, enc, SaverConfig(pathStr, NJFileFormat.BinaryAvro).withSingle)

  def binAvro[K: ClassTag: Eq](bucketing: A => K, pathBuilder: K => String)(implicit
    enc: AvroEncoder[A]): BinaryAvroPartitionSaver[F, A, K] =
    new BinaryAvroPartitionSaver[F, A, K](
      rdd,
      enc,
      SaverConfig("", NJFileFormat.Avro),
      bucketing,
      pathBuilder)

// 4
  def parquet(pathStr: String)(implicit
    enc: AvroEncoder[A],
    constraint: TypedEncoder[A]): ParquetSaver[F, A] =
    new ParquetSaver[F, A](rdd, enc, constraint, SaverConfig(pathStr, NJFileFormat.Parquet))

// 5
  def circe(pathStr: String)(implicit enc: JsonEncoder[A]): CirceSaver[F, A] =
    new CirceSaver[F, A](rdd, enc, SaverConfig(pathStr, NJFileFormat.Circe))

  def circe[K: ClassTag: Eq](bucketing: A => K, pathBuilder: K => String)(implicit
    enc: JsonEncoder[A]): CircePartitionSaver[F, A, K] =
    new CircePartitionSaver[F, A, K](
      rdd,
      enc,
      SaverConfig("", NJFileFormat.Avro),
      bucketing,
      pathBuilder)

// 6
  def text(pathStr: String)(implicit enc: Show[A]): TextSaver[F, A] =
    new TextSaver[F, A](rdd, enc, SaverConfig(pathStr, NJFileFormat.Text))

// 7
  def csv(
    pathStr: String)(implicit enc: RowEncoder[A], constraint: TypedEncoder[A]): CsvSaver[F, A] =
    new CsvSaver[F, A](
      rdd,
      enc,
      CsvConfiguration.rfc,
      constraint,
      SaverConfig(pathStr, NJFileFormat.Csv))

// 8
  def javaObject(pathStr: String): JavaObjectSaver[F, A] =
    new JavaObjectSaver[F, A](rdd, SaverConfig(pathStr, NJFileFormat.JavaObject).withSingle)

// 9
  def protobuf(pathStr: String)(implicit ev: A <:< GeneratedMessage): ProtobufSaver[F, A] =
    new ProtobufSaver[F, A](rdd, SaverConfig(pathStr, NJFileFormat.ProtoBuf).withSingle)

// 10
  def dump(pathStr: String): Dumper[F, A] =
    new Dumper[F, A](rdd, pathStr)
}
