package com.github.chenharryhua.nanjin.spark

import com.github.chenharryhua.nanjin.spark.persist.loaders
import com.sksamuel.avro4s.Decoder as AvroDecoder
import io.circe.Decoder as JsonDecoder
import io.lemonlabs.uri.Url
import kantan.csv.{CsvConfiguration, RowDecoder}
import org.apache.spark.sql.{Dataset, Encoder as SparkEncoder, SparkSession}

import scala.reflect.ClassTag

final class LoadDataset[A: ClassTag: SparkEncoder] private[spark] (path: Url, ss: SparkSession) {

  def parquet(implicit dec: AvroDecoder[A]): Dataset[A] = loaders.parquet[A](path, ss)

  def avro(implicit decoder: AvroDecoder[A]): Dataset[A] =
    loaders.avro[A](path, ss)

  def circe(implicit ev: JsonDecoder[A]): Dataset[A] =
    loaders.circe[A](path, ss)

  def kantan(cfg: CsvConfiguration)(implicit dec: RowDecoder[A]): Dataset[A] =
    loaders.kantan[A](path, ss, cfg)

  def jackson(implicit dec: AvroDecoder[A]): Dataset[A] = loaders.jackson[A](path, ss)

  def binAvro(implicit dec: AvroDecoder[A]): Dataset[A] = loaders.binAvro[A](path, ss)

  def objectFile: Dataset[A] = loaders.objectFile(path, ss)

}
