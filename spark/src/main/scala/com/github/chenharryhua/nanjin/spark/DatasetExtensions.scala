package com.github.chenharryhua.nanjin.spark

import cats.effect.Concurrent
import cats.implicits._
import com.github.chenharryhua.nanjin.kafka.Keyboard
import frameless.cats.implicits._
import frameless.{TypedDataset, TypedEncoder}
import fs2.Stream
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

private[spark] trait DatasetExtensions {

  implicit class TypedDatasetExt[A](private val tds: TypedDataset[A]) {

    def stream[F[_]: Concurrent]: Stream[F, A] =
      for {
        kb <- Keyboard.signal[F]
        data <- Stream
          .eval(
            tds.toLocalIterator.map(it => Stream.fromIterator[F](it.asScala.flatMap(Option(_)))))
          .flatten
          .pauseWhen(kb.map(_.contains(Keyboard.pauSe)))
          .interruptWhen(kb.map(_.contains(Keyboard.Quit)))
      } yield data
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
