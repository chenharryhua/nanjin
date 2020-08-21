package com.github.chenharryhua.nanjin.spark.database

import cats.effect.Sync
import org.apache.spark.rdd.RDD

private[database] trait DatasetExtensions {

  implicit final class SparkDBSyntax[A](val rdd: RDD[A]) {

    def upload[F[_]: Sync](db: SparkTable[F, A]): DbUploader[F, A] =
      db.tableDataset(rdd).upload
  }
}
