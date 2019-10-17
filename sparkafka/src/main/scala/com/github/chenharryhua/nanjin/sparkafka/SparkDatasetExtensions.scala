package com.github.chenharryhua.nanjin.sparkafka

import com.github.chenharryhua.nanjin.sparkdb.TableDataset
import frameless.TypedDataset

private[sparkafka] trait SparkDatasetExtensions {

  implicit final class SparkDBSyntax[A](data: TypedDataset[A]) {
    def dbUpload[F[_]](db: TableDataset[F, A]): F[Unit] = db.uploadToDB(data)
  }
}
