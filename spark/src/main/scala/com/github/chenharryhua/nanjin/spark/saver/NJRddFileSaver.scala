package com.github.chenharryhua.nanjin.spark.saver

import cats.Show
import com.sksamuel.avro4s.{Encoder => AvroEncoder}
import frameless.TypedEncoder
import io.circe.{Encoder => JsonEncoder}
import kantan.csv.{CsvConfiguration, RowEncoder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode
import scalapb.GeneratedMessage

import scala.reflect.ClassTag

final class NJRddFileSaver[F[_], A](rdd: RDD[A]) extends Serializable {

  def map[B: ClassTag](f: A => B): NJRddFileSaver[F, B] =
    new NJRddFileSaver[F, B](rdd.map(f))

  def flatMap[B: ClassTag](f: A => TraversableOnce[B]): NJRddFileSaver[F, B] =
    new NJRddFileSaver[F, B](rdd.flatMap(f))

  private val defaultSaveMode: SaveMode = SaveMode.Overwrite
  private val defaultSM: SingleOrMulti  = SingleOrMulti.Multi
  private val defaultSH: SparkOrHadoop  = SparkOrHadoop.Hadoop

// 1
  def avro(pathStr: String)(implicit enc: AvroEncoder[A]): AvroSaver[F, A] =
    AvroSaver[F, A](rdd, enc, pathStr, defaultSaveMode, defaultSM, defaultSH)

// 2
  def jackson(pathStr: String)(implicit enc: AvroEncoder[A]): JacksonSaver[F, A] =
    JacksonSaver[F, A](rdd, enc, pathStr, defaultSaveMode, defaultSM)

// 3
  def binAvro(pathStr: String)(implicit enc: AvroEncoder[A]): BinaryAvroSaver[F, A] =
    BinaryAvroSaver[F, A](rdd, enc, pathStr, defaultSaveMode)

// 4
  def parquet(pathStr: String)(implicit
    enc: AvroEncoder[A],
    constraint: TypedEncoder[A]): ParquetSaver[F, A] =
    ParquetSaver[F, A](rdd, enc, pathStr, defaultSaveMode, defaultSM, defaultSH, constraint)

// 5
  def circe(pathStr: String)(implicit enc: JsonEncoder[A]): CirceJsonSaver[F, A] =
    CirceJsonSaver[F, A](rdd, enc, pathStr, defaultSaveMode, defaultSM)

// 6
  def text(pathStr: String)(implicit enc: Show[A]): TextSaver[F, A] =
    TextSaver[F, A](rdd, enc, pathStr, defaultSaveMode, defaultSM)

// 7
  def csv(
    pathStr: String)(implicit enc: RowEncoder[A], constraint: TypedEncoder[A]): CsvSaver[F, A] =
    CsvSaver[F, A](rdd, enc, CsvConfiguration.rfc, pathStr, defaultSaveMode, defaultSM, constraint)

// 8
  def protobuf(pathStr: String)(implicit ev: A <:< GeneratedMessage): ProtobufSaver[F, A] =
    new ProtobufSaver[F, A](rdd, pathStr)

// 9
  def javaObject(pathStr: String): JavaObjectSaver[F, A] =
    new JavaObjectSaver[F, A](rdd, pathStr, defaultSaveMode)

// 10
  def dump(pathStr: String): Dumper[F, A] =
    new Dumper[F, A](rdd, pathStr)
}
