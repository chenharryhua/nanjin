package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.kernel.Sync
import com.github.chenharryhua.nanjin.common.ChunkSize
import com.github.chenharryhua.nanjin.common.NJFileFormat.*
import com.github.chenharryhua.nanjin.spark.RddExt
import com.sksamuel.avro4s.Encoder as AvroEncoder
import fs2.Stream
import kantan.csv.CsvConfiguration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset

sealed class RddFileHoarder[F[_], A](rdd: RDD[A]) extends Serializable {

// 1
  final def circe(path: String): SaveCirce[F, A] =
    new SaveCirce[F, A](rdd, HoarderConfig(path).outputFormat(Circe), isKeepNull = true)

// 2
  final def text(path: String): SaveText[F, A] =
    new SaveText[F, A](rdd, HoarderConfig(path).outputFormat(Text), Text.suffix)

// 3
  final def objectFile(path: String): SaveObjectFile[F, A] =
    new SaveObjectFile[F, A](rdd, HoarderConfig(path).outputFormat(JavaObject))

// 4
  final def protobuf(path: String): SaveProtobuf[F, A] =
    new SaveProtobuf[F, A](rdd, HoarderConfig(path).outputFormat(ProtoBuf))

  final def stream(chunkSize: ChunkSize)(implicit F: Sync[F]): Stream[F, A] = rdd.stream[F](chunkSize)
}

sealed class RddAvroFileHoarder[F[_], A](rdd: RDD[A], encoder: AvroEncoder[A]) extends RddFileHoarder[F, A](rdd) {

// 1
  final def jackson(path: String): SaveJackson[F, A] =
    new SaveJackson[F, A](rdd, encoder, HoarderConfig(path).outputFormat(Jackson))

// 2
  final def avro(path: String): SaveAvro[F, A] =
    new SaveAvro[F, A](rdd, encoder, HoarderConfig(path).outputFormat(Avro))

// 3
  final def binAvro(path: String): SaveBinaryAvro[F, A] =
    new SaveBinaryAvro[F, A](rdd, encoder, HoarderConfig(path).outputFormat(BinaryAvro))

}

final class DatasetFileHoarder[F[_], A](ds: Dataset[A]) extends RddFileHoarder[F, A](ds.rdd) {

// 1
  def csv(path: String): SaveCsv[F, A] =
    new SaveCsv[F, A](ds, CsvConfiguration.rfc, HoarderConfig(path).outputFormat(Csv))

// 2
  def json(path: String): SaveSparkJson[F, A] =
    new SaveSparkJson[F, A](ds, HoarderConfig(path).outputFormat(SparkJson), isKeepNull = true)

}

final class DatasetAvroFileHoarder[F[_], A](ds: Dataset[A], encoder: AvroEncoder[A])
    extends RddAvroFileHoarder[F, A](ds.rdd, encoder) {

  // 1
  def csv(path: String): SaveCsv[F, A] =
    new SaveCsv[F, A](ds, CsvConfiguration.rfc, HoarderConfig(path).outputFormat(Csv))

  // 2
  def json(path: String): SaveSparkJson[F, A] =
    new SaveSparkJson[F, A](ds, HoarderConfig(path).outputFormat(SparkJson), isKeepNull = true)

  // 3
  def parquet(path: String): SaveParquet[F, A] =
    new SaveParquet[F, A](ds, encoder, HoarderConfig(path).outputFormat(Parquet))

}
