package com.github.chenharryhua.nanjin.spark.saver

import cats.Show
import com.sksamuel.avro4s.{Encoder => AvroEncoder}
import frameless.TypedEncoder
import io.circe.{Encoder => JsonEncoder}
import kantan.csv.{CsvConfiguration, RowEncoder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode
import scalapb.GeneratedMessage

final class NJRddFileSaver[F[_], A](rdd: RDD[A]) extends Serializable {

// 1
  def avro(pathStr: String)(implicit enc: AvroEncoder[A]): AvroSaver[F, A] =
    AvroSaver[F, A](
      rdd,
      enc,
      pathStr,
      SaveMode.Overwrite,
      SingleOrMulti.Multi,
      SparkOrHadoop.Hadoop)

// 2
  def jackson(pathStr: String)(implicit enc: AvroEncoder[A]): JacksonSaver[F, A] =
    JacksonSaver[F, A](rdd, enc, pathStr, SaveMode.Overwrite, SingleOrMulti.Multi)

// 3
  def binAvro(pathStr: String)(implicit enc: AvroEncoder[A]): BinaryAvroSaver[F, A] =
    BinaryAvroSaver[F, A](rdd, enc, pathStr, SaveMode.Overwrite)

// 4
  def parquet(pathStr: String)(implicit
    enc: AvroEncoder[A],
    constraint: TypedEncoder[A]): ParquetSaver[F, A] =
    ParquetSaver[F, A](
      rdd,
      enc,
      pathStr,
      SaveMode.Overwrite,
      SingleOrMulti.Multi,
      SparkOrHadoop.Hadoop,
      constraint)

// 5
  def circe(pathStr: String)(implicit enc: JsonEncoder[A]): CirceJsonSaver[F, A] =
    CirceJsonSaver[F, A](rdd, enc, pathStr, SaveMode.Overwrite, SingleOrMulti.Multi)

// 6
  def text(pathStr: String)(implicit enc: Show[A]): TextSaver[F, A] =
    TextSaver[F, A](rdd, enc, pathStr, SaveMode.Overwrite, SingleOrMulti.Multi)

// 7
  def csv(
    pathStr: String)(implicit enc: RowEncoder[A], constraint: TypedEncoder[A]): CsvSaver[F, A] =
    CsvSaver[F, A](
      rdd,
      enc,
      CsvConfiguration.rfc,
      pathStr,
      SaveMode.Overwrite,
      SingleOrMulti.Multi,
      constraint)

// 8
  def protobuf(pathStr: String)(implicit ev: A <:< GeneratedMessage) =
    new ProtobufSaver[F, A](rdd, pathStr)

// 9
  def javaObject(pathStr: String): JavaObjectSaver[F, A] =
    new JavaObjectSaver[F, A](rdd, pathStr)

// 10
  def dump(pathStr: String): Dumper[F, A] =
    new Dumper[F, A](rdd, pathStr)
}
