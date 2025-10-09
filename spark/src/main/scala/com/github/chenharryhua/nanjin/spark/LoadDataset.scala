package com.github.chenharryhua.nanjin.spark

import com.github.chenharryhua.nanjin.spark.persist.loaders
import com.sksamuel.avro4s.Decoder as AvroDecoder
import io.circe.Decoder as JsonDecoder
import io.lemonlabs.uri.Url
import kantan.csv.{CsvConfiguration, RowDecoder}
import org.apache.spark.sql.{Dataset, Encoder as SparkEncoder, SparkSession}

import scala.reflect.ClassTag

final class LoadDataset[A: SparkEncoder] private[spark] (path: Url, ss: SparkSession) {
  implicit private val clsTag: ClassTag[A] = implicitly[SparkEncoder[A]].clsTag

  def parquet(implicit dec: AvroDecoder[A]): Dataset[A] =
    ss.createDataset(loaders.parquet[A](path, ss))

  def avro(implicit dec: AvroDecoder[A]): Dataset[A] =
    ss.createDataset(loaders.avro[A](path, ss))

  def circe(implicit dec: JsonDecoder[A]): Dataset[A] =
    ss.createDataset(loaders.circe[A](path, ss))

  def kantan(cfg: CsvConfiguration)(implicit dec: RowDecoder[A]): Dataset[A] =
    ss.createDataset(loaders.kantan[A](path, ss, cfg))

  def jackson(implicit dec: AvroDecoder[A]): Dataset[A] =
    ss.createDataset(loaders.jackson[A](path, ss))

  def binAvro(implicit dec: AvroDecoder[A]): Dataset[A] =
    ss.createDataset(loaders.binAvro[A](path, ss))

  def objectFile: Dataset[A] =
    ss.createDataset(loaders.objectFile(path, ss))

}
