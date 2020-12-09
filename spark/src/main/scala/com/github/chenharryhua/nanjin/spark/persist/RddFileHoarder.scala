package com.github.chenharryhua.nanjin.spark.persist

import com.github.chenharryhua.nanjin.common.NJFileFormat._
import com.sksamuel.avro4s.{Encoder => AvroEncoder}
import kantan.csv.CsvConfiguration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset

final class RddFileHoarder[F[_], A] private (rdd: RDD[A], cfg: HoarderConfig) extends Serializable {

  private def updateConfig(cfg: HoarderConfig): RddFileHoarder[F, A] =
    new RddFileHoarder[F, A](rdd, cfg)

// 1
  def circe: SaveCirce[F, A] =
    new SaveCirce[F, A](rdd, cfg.withFormat(Circe), isKeepNull = true)

// 2
  def text: SaveText[F, A] =
    new SaveText[F, A](rdd, cfg.withFormat(Text), Text.suffix)

// 3
  def objectFile: SaveObjectFile[F, A] =
    new SaveObjectFile[F, A](rdd, cfg.withFormat(JavaObject))

// 4
  def protobuf: SaveProtobuf[F, A] =
    new SaveProtobuf[F, A](rdd, cfg.withFormat(ProtoBuf))
}

object RddFileHoarder {

  def apply[F[_], A](rdd: RDD[A], outPath: String) =
    new RddFileHoarder[F, A](rdd, HoarderConfig(outPath))
}

final class AvroFileHoarder[F[_], A] private (
  rdd: RDD[A],
  cfg: HoarderConfig,
  encoder: AvroEncoder[A])
    extends Serializable {

  private def updateConfig(cfg: HoarderConfig): AvroFileHoarder[F, A] =
    new AvroFileHoarder[F, A](rdd, cfg, encoder)

// 1
  def jackson: SaveJackson[F, A] =
    new SaveJackson[F, A](rdd, encoder, cfg.withFormat(Jackson))

// 2
  def avro: SaveAvro[F, A] =
    new SaveAvro[F, A](rdd, encoder, cfg.withFormat(Avro))

// 3
  def binAvro: SaveBinaryAvro[F, A] =
    new SaveBinaryAvro[F, A](rdd, encoder, cfg.withFormat(BinaryAvro))

}

object AvroFileHoarder {

  def apply[F[_], A](rdd: RDD[A], outPath: String, encoder: AvroEncoder[A]) =
    new AvroFileHoarder[F, A](rdd, HoarderConfig(outPath), encoder)
}

final class DatasetFileHoarder[F[_], A] private (ds: Dataset[A], cfg: HoarderConfig)
    extends Serializable {

  private def updateConfig(cfg: HoarderConfig): DatasetFileHoarder[F, A] =
    new DatasetFileHoarder[F, A](ds, cfg)

// 1
  def csv: SaveCsv[F, A] =
    new SaveCsv[F, A](ds, CsvConfiguration.rfc, cfg.withFormat(Csv))

// 2
  def json: SaveSparkJson[F, A] =
    new SaveSparkJson[F, A](ds, cfg.withFormat(SparkJson), isKeepNull = true)

// 3
  def parquet: SaveParquet[F, A] =
    new SaveParquet[F, A](ds, cfg.withFormat(Parquet))

}

object DatasetFileHoarder {

  def apply[F[_], A](ds: Dataset[A], outPath: String) =
    new DatasetFileHoarder[F, A](ds, HoarderConfig(outPath))

}
