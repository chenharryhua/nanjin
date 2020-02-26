package com.github.chenharryhua.nanjin.spark

import cats.effect.Sync
import cats.implicits._
import frameless.TypedDataset
import frameless.cats.implicits._
import fs2.Stream
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._

private[spark] trait DatasetExtensions {

  implicit class RddExt[A](private val rdd: RDD[A]) {

    def stream[F[_]: Sync]: Stream[F, A] =
      Stream.fromIterator(rdd.toLocalIterator)
  }

  implicit class TypedDatasetExt[A](private val tds: TypedDataset[A]) {

    def stream[F[_]: Sync]: Stream[F, A] =
      Stream.force(
        tds.toLocalIterator.map(it => Stream.fromIterator[F](it.asScala.flatMap(Option(_)))))
  }

}
