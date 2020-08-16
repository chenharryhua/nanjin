package com.github.chenharryhua.nanjin.spark.database

import cats.effect.Sync
import org.apache.spark.rdd.RDD

private[database] trait DatasetExtensions {

  implicit final class SparkDBSyntax[A](val rdd: RDD[A]) {

    def dbUpload[F[_]: Sync](db: SparkTable[F, A]): F[Unit] =
      db.tableDataset(rdd).upload
  }
}
