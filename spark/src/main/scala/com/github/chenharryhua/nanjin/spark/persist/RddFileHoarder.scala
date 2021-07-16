package com.github.chenharryhua.nanjin.spark.persist

import com.github.chenharryhua.nanjin.common.NJFileFormat.*
import com.sksamuel.avro4s.Encoder as AvroEncoder
import kantan.csv.CsvConfiguration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset

sealed class RddFileHoarder[F[_], A](rdd: RDD[A]) extends Serializable {

// 1
  final def circe(path: String): SaveCirce[F, A] =
    new SaveCirce[F, A](rdd, HoarderConfig(path).output_format(Circe), isKeepNull = true)

// 2
  final def text(path: String): SaveText[F, A] =
    new SaveText[F, A](rdd, HoarderConfig(path).output_format(Text), Text.suffix)

// 3
  final def objectFile(path: String): SaveObjectFile[F, A] =
    new SaveObjectFile[F, A](rdd, HoarderConfig(path).output_format(JavaObject))

// 4
  final def protobuf(path: String): SaveProtobuf[F, A] =
    new SaveProtobuf[F, A](rdd, HoarderConfig(path).output_format(ProtoBuf))
}

sealed class RddAvroFileHoarder[F[_], A](rdd: RDD[A], encoder: AvroEncoder[A]) extends RddFileHoarder[F, A](rdd) {

// 1
  final def jackson(path: String): SaveJackson[F, A] =
    new SaveJackson[F, A](rdd, encoder, HoarderConfig(path).output_format(Jackson))

// 2
  final def avro(path: String): SaveAvro[F, A] =
    new SaveAvro[F, A](rdd, encoder, HoarderConfig(path).output_format(Avro))

// 3
  final def binAvro(path: String): SaveBinaryAvro[F, A] =
    new SaveBinaryAvro[F, A](rdd, encoder, HoarderConfig(path).output_format(BinaryAvro))

}

final class DatasetFileHoarder[F[_], A](ds: Dataset[A]) extends RddFileHoarder[F, A](ds.rdd) {

// 1
  def csv(path: String): SaveCsv[F, A] =
    new SaveCsv[F, A](ds, CsvConfiguration.rfc, HoarderConfig(path).output_format(Csv))

// 2
  def json(path: String): SaveSparkJson[F, A] =
    new SaveSparkJson[F, A](ds, HoarderConfig(path).output_format(SparkJson), isKeepNull = true)

}

final class DatasetAvroFileHoarder[F[_], A](ds: Dataset[A], encoder: AvroEncoder[A])
    extends RddAvroFileHoarder[F, A](ds.rdd, encoder) {

  // 1
  def csv(path: String): SaveCsv[F, A] =
    new SaveCsv[F, A](ds, CsvConfiguration.rfc, HoarderConfig(path).output_format(Csv))

  // 2
  def json(path: String): SaveSparkJson[F, A] =
    new SaveSparkJson[F, A](ds, HoarderConfig(path).output_format(SparkJson), isKeepNull = true)

  // 3
  def parquet(path: String): SaveParquet[F, A] =
    new SaveParquet[F, A](ds, encoder, HoarderConfig(path).output_format(Parquet))

}
