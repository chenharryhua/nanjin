package com.github.chenharryhua.nanjin.spark

import cats.effect.Sync
import frameless.TypedDataset
import fs2.Stream

import scala.collection.JavaConverters._

private[spark] trait DatasetExtensions {

  implicit class TypedDatasetExt[A](tds: TypedDataset[A]) {

    def stream[F[_]: Sync]: Stream[F, A] =
      Stream.fromIterator[F](tds.dataset.toLocalIterator().asScala)

  }

}
