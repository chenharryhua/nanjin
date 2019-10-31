package com.github.chenharryhua.nanjin.spark

import cats.effect.Sync
import cats.implicits._
import frameless.cats.implicits._
import frameless.{TypedDataset, TypedEncoder}
import fs2.Stream
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

private[spark] trait DatasetExtensions {

  implicit class TypedDatasetExt[A](private val tds: TypedDataset[A]) {

    def stream[F[_]: Sync]: Stream[F, A] =
      Stream.eval(tds.toLocalIterator.map(it => Stream.fromIterator[F](it.asScala))).flatten
  }

  implicit class NJSparkSessionExt(private val sparkSession: SparkSession) {

    def parquet[A: TypedEncoder](path: String): TypedDataset[A] =
      TypedDataset.createUnsafe[A](sparkSession.read.parquet(path))

    def csv[A: TypedEncoder](
      path: String,
      params: FileFormat.Csv = FileFormat.Csv.default): TypedDataset[A] =
      TypedDataset.createUnsafe[A](sparkSession.read.options(params.options).csv(path))
  }
}
