package com.github.chenharryhua.nanjin.spark

import com.github.chenharryhua.nanjin.spark.persist.loaders
import com.sksamuel.avro4s.Decoder as AvroDecoder
import io.circe.Decoder as JsonDecoder
import io.lemonlabs.uri.Url
import kantan.csv.{CsvConfiguration, RowDecoder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

final class LoadRdd[A: ClassTag] private[spark] (path: Url, ss: SparkSession) {
  def circe(implicit ev: JsonDecoder[A]): RDD[A] =
    loaders.rdd.circe[A](path, ss)

  def kantan(cfg: CsvConfiguration)(implicit dec: RowDecoder[A]): RDD[A] =
    loaders.rdd.kantan[A](path, ss, cfg)

  def objectFile: RDD[A] =
    loaders.rdd.objectFile[A](path, ss)

  def avro(implicit dec: AvroDecoder[A]): RDD[A] =
    loaders.rdd.avro[A](path, ss)

  def jackson(implicit dec: AvroDecoder[A]): RDD[A] =
    loaders.rdd.jackson[A](path, ss)

  def binAvro(implicit dec: AvroDecoder[A]): RDD[A] =
    loaders.rdd.binAvro[A](path, ss)

  def parquet(implicit dec: AvroDecoder[A]): RDD[A] =
    loaders.rdd.parquet[A](path, ss)

}
