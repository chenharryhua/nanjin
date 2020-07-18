package com.github.chenharryhua.nanjin.spark.database

import frameless.TypedDataset

private[database] trait DatasetExtensions {

  implicit final class SparkDBSyntax[A](val data: TypedDataset[A]) {
    def dbUpload[F[_]](db: SparkTable[A]): Unit = db.upload(data)
  }
}
