package com.github.chenharryhua.nanjin.spark.saver

import cats.Show
import com.sksamuel.avro4s.{Encoder => AvroEncoder}
import frameless.TypedEncoder
import io.circe.{Encoder => JsonEncoder}
import kantan.csv.{CsvConfiguration, RowEncoder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode
import scalapb.GeneratedMessage

final class NJRddFileSaver[A](rdd: RDD[A]) extends Serializable {

// 1
  def avro(pathStr: String)(implicit enc: AvroEncoder[A]): AvroSaver[A] =
    AvroSaver[A](rdd, enc, pathStr, SaveMode.Overwrite, SingleOrMulti.Multi, SparkOrHadoop.Hadoop)

// 2
  def jackson(pathStr: String)(implicit enc: AvroEncoder[A]): JacksonSaver[A] =
    JacksonSaver[A](rdd, enc, pathStr, SaveMode.Overwrite, SingleOrMulti.Multi)

// 3
  def binAvro(pathStr: String)(implicit enc: AvroEncoder[A]): BinaryAvroSaver[A] =
    BinaryAvroSaver[A](rdd, enc, pathStr, SaveMode.Overwrite)

// 4
  def parquet(
    pathStr: String)(implicit enc: AvroEncoder[A], constraint: TypedEncoder[A]): ParquetSaver[A] =
    ParquetSaver[A](
      rdd,
      enc,
      pathStr,
      SaveMode.Overwrite,
      SingleOrMulti.Multi,
      SparkOrHadoop.Hadoop,
      constraint)

// 5
  def circe(pathStr: String)(implicit enc: JsonEncoder[A]): CirceJsonSaver[A] =
    CirceJsonSaver[A](rdd, enc, pathStr, SaveMode.Overwrite, SingleOrMulti.Multi)

// 6
  def text(pathStr: String)(implicit enc: Show[A]): TextSaver[A] =
    TextSaver[A](rdd, enc, pathStr, SaveMode.Overwrite, SingleOrMulti.Multi)

// 7
  def csv(pathStr: String)(implicit enc: RowEncoder[A], constraint: TypedEncoder[A]): CsvSaver[A] =
    CsvSaver[A](
      rdd,
      enc,
      CsvConfiguration.rfc,
      pathStr,
      SaveMode.Overwrite,
      SingleOrMulti.Multi,
      constraint)

// 8
  def protobuf(pathStr: String)(implicit ev: A <:< GeneratedMessage) =
    new ProtobufSaver[A](rdd, pathStr)

// 9
  def javaObject(pathStr: String): JavaObjectSaver[A] =
    new JavaObjectSaver[A](rdd, pathStr)

// 10
  def dump(pathStr: String): Dumper[A] =
    new Dumper[A](rdd, pathStr)
}
