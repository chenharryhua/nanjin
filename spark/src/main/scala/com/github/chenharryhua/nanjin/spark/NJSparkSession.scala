package com.github.chenharryhua.nanjin.spark

import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoder

final case class NJSparkSession(sparkSession: SparkSession) {
  implicit private val spks: SparkSession = sparkSession
  import sparkSession.implicits._

  def parquet[A: TypedEncoder: Encoder](path: String): TypedDataset[A] =
    TypedDataset.create[A](sparkSession.read.parquet(path).as[A])

  def csv[A: TypedEncoder: Encoder](
    path: String,
    params: FileFormat.Csv = FileFormat.Csv.default): TypedDataset[A] =
    TypedDataset.create[A](sparkSession.read.options(params.options).csv(path).as[A])

}
