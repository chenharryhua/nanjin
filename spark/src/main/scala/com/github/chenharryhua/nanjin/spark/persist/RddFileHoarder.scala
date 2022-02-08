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

sealed class RddFileHoarder[F[_], A](rdd: RDD[A], cfg: HoarderConfig) extends Serializable {

// 1
  final def circe: SaveCirce[F, A] = new SaveCirce[F, A](rdd, cfg.outputFormat(Circe), isKeepNull = true)

// 2
  final def text: SaveText[F, A] = new SaveText[F, A](rdd, cfg.outputFormat(Text), Text.suffix)

// 3
  final def objectFile: SaveObjectFile[F, A] = new SaveObjectFile[F, A](rdd, cfg.outputFormat(JavaObject))

// 4
  final def protobuf: SaveProtobuf[F, A] = new SaveProtobuf[F, A](rdd, cfg.outputFormat(ProtoBuf))

  final def stream(chunkSize: ChunkSize)(implicit F: Sync[F]): Stream[F, A] = rdd.stream[F](chunkSize)
}

sealed class RddAvroFileHoarder[F[_], A](rdd: RDD[A], encoder: AvroEncoder[A], cfg: HoarderConfig)
    extends RddFileHoarder[F, A](rdd, cfg) {

// 1
  final def jackson: SaveJackson[F, A] = new SaveJackson[F, A](rdd, encoder, cfg.outputFormat(Jackson))

// 2
  final def avro: SaveAvro[F, A] = new SaveAvro[F, A](rdd, encoder, cfg.outputFormat(Avro))

// 3
  final def binAvro: SaveBinaryAvro[F, A] = new SaveBinaryAvro[F, A](rdd, encoder, cfg.outputFormat(BinaryAvro))

}

final class DatasetFileHoarder[F[_], A](ds: Dataset[A], cfg: HoarderConfig) extends RddFileHoarder[F, A](ds.rdd, cfg) {

  // 1
  def csv: SaveCsv[F, A] = new SaveCsv[F, A](ds, CsvConfiguration.rfc, cfg.outputFormat(Csv))

  // 2
  def json: SaveSparkJson[F, A] = new SaveSparkJson[F, A](ds, cfg.outputFormat(SparkJson), isKeepNull = true)

  // 3
  def parquet: SaveParquet[F, A] = new SaveParquet[F, A](ds, cfg.outputFormat(Parquet))
}

final class DatasetAvroFileHoarder[F[_], A](ds: Dataset[A], encoder: AvroEncoder[A], cfg: HoarderConfig)
    extends RddAvroFileHoarder[F, A](ds.rdd, encoder, cfg) {

  // 1
  def csv: SaveCsv[F, A] = new SaveCsv[F, A](ds, CsvConfiguration.rfc, cfg.outputFormat(Csv))

  // 2
  def json: SaveSparkJson[F, A] = new SaveSparkJson[F, A](ds, cfg.outputFormat(SparkJson), isKeepNull = true)

  // 3
  def parquet: SaveParquet[F, A] = new SaveParquet[F, A](ds, cfg.outputFormat(Parquet))

}
