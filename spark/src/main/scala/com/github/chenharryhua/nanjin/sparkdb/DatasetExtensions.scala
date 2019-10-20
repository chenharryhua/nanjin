package com.github.chenharryhua.nanjin.sparkdb

import frameless.TypedDataset

private[sparkdb] trait DatasetExtensions {

  implicit final class SparkDBSyntax[A](data: TypedDataset[A]) {
    def dbUpload[F[_]](db: SparkTableSession[F, A]): F[Unit] = db.uploadToDB(data)
  }
}
