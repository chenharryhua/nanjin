package com.github.chenharryhua.nanjin.spark.database

import frameless.TypedDataset

private[database] trait DatasetExtensions {

  implicit final class SparkDBSyntax[A](data: TypedDataset[A]) {
    def dbUpload[F[_]](db: SparkTableSession[F, A]): F[Unit] = db.uploadToDB(data)
  }
}
