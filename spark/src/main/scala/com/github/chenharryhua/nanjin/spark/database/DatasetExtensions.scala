package com.github.chenharryhua.nanjin.spark.database

import cats.effect.Sync
import org.apache.spark.sql.Dataset

private[database] trait DatasetExtensions {

  implicit final class SparkDBSyntax[A](ds: Dataset[A]) extends Serializable {

    def upload[F[_]: Sync](db: SparkTable[F, A]): DbUploader[F, A] =
      db.tableset(ds).upload
  }
}
