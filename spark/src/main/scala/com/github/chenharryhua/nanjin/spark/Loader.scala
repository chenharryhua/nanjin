package com.github.chenharryhua.nanjin.spark

import com.github.chenharryhua.nanjin.spark.persist.loaders
import com.sksamuel.avro4s.Decoder as AvroDecoder
import io.circe.Decoder as JsonDecoder
import io.lemonlabs.uri.Url
import kantan.csv.{CsvConfiguration, RowDecoder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

sealed class LoadRDD[A: ClassTag](ss: SparkSession, path: Url) {
  def circe(implicit ev: JsonDecoder[A]): RDD[A] = loaders.rdd.circe[A](path, ss)

  def kantan(cfg: CsvConfiguration)(implicit dec: RowDecoder[A]): RDD[A] =
    loaders.rdd.kantan[A](path, ss, cfg)

  def objectFile: RDD[A] = loaders.rdd.objectFile[A](path, ss)
}

final class LoadAvroRDD[A: ClassTag](ss: SparkSession, path: Url, decoder: AvroDecoder[A])
    extends LoadRDD(ss, path) {
  def avro: RDD[A]    = loaders.rdd.avro(path, ss, decoder)
  def jackson: RDD[A] = loaders.rdd.jackson(path, ss, decoder)
  def binAvro: RDD[A] = loaders.rdd.binAvro(path, ss, decoder)
  def parquet: RDD[A] = loaders.rdd.parquet(path, ss, decoder)
}
