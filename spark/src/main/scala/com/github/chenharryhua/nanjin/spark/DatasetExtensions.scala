package com.github.chenharryhua.nanjin.spark

import cats.effect.Concurrent
import cats.implicits._
import com.github.chenharryhua.nanjin.utils.Keyboard
import frameless.TypedDataset
import frameless.cats.implicits._
import fs2.Stream

import scala.collection.JavaConverters._

private[spark] trait DatasetExtensions {

  implicit class TypedDatasetExt[A](private val tds: TypedDataset[A]) {

    def stream[F[_]: Concurrent]: Stream[F, A] =
      for {
        kb <- Keyboard.signal[F]
        data <- Stream
          .force(
            tds.toLocalIterator.map(it => Stream.fromIterator[F](it.asScala.flatMap(Option(_)))))
          .pauseWhen(kb.map(_.contains(Keyboard.pauSe)))
          .interruptWhen(kb.map(_.contains(Keyboard.Quit)))
      } yield data
  }
}
