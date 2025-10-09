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

  def kantan(cfg: CsvConfiguration)(implicit dec: RowDecoder[A]): RDD[A] =
    loaders.kantan[A](path, ss, cfg)

  def circe(implicit dec: JsonDecoder[A]): RDD[A] = loaders.circe[A](path, ss)
  def objectFile: RDD[A] = loaders.objectFile[A](path, ss)
  def avro(implicit dec: AvroDecoder[A]): RDD[A] = loaders.avro[A](path, ss)
  def jackson(implicit dec: AvroDecoder[A]): RDD[A] = loaders.jackson[A](path, ss)
  def binAvro(implicit dec: AvroDecoder[A]): RDD[A] = loaders.binAvro[A](path, ss)
  def parquet(implicit dec: AvroDecoder[A]): RDD[A] = loaders.parquet[A](path, ss)
}
