package com.github.chenharryhua.nanjin.spark

import cats.effect.{Concurrent, Sync}
import cats.implicits._
import com.github.chenharryhua.nanjin.utils.Keyboard
import frameless.cats.implicits._
import frameless.{TypedDataset, TypedEncoder}
import fs2.Stream
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.DataStreamWriter

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

  implicit final class SparkDataStreamWriterSyntax[A](private val dsw: DataStreamWriter[A]) {

    def run[F[_]](implicit F: Sync[F]): F[Unit] =
      F.bracket(F.delay(dsw.start))(s => F.delay(s.awaitTermination()))(_ => F.pure(()))
  }
}
