package com.github.chenharryhua.nanjin.spark

import cats.effect.Concurrent
import cats.implicits._
import com.github.chenharryhua.nanjin.utils.Keyboard
import com.sksamuel.avro4s._
import frameless.TypedDataset
import frameless.cats.implicits._
import fs2.Stream
import org.apache.spark.sql.SparkSession

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

    def saveJackson[F[_]: Concurrent, B: SchemaFor: Encoder](pathStr: String)(f: A => B)(
      implicit sparkSession: SparkSession): Stream[F, Unit] =
      tds.stream[F].through(jacksonSink[F, A, B](pathStr)(f))

    def saveAvro[F[_]: Concurrent, B: SchemaFor: Encoder](pathStr: String)(f: A => B)(
      implicit sparkSession: SparkSession): Stream[F, Unit] =
      tds.stream.through(avroSink[F, A, B](pathStr)(f))
  }

  implicit class SparkSessionExt(private val sks: SparkSession) {

    def loadAvro[F[_], A](pathStr: String)(
      implicit
      concurrent: Concurrent[F],
      decoder: Decoder[A],
      schemaFor: SchemaFor[A]): Stream[F, A] =
      avroSource[F, A](pathStr)(schemaFor, decoder, sks, concurrent)

    def loadJackson[F[_], A](pathStr: String)(
      implicit
      concurrent: Concurrent[F],
      decoder: Decoder[A],
      schemaFor: SchemaFor[A]): Stream[F, A] =
      jacksonSource[F, A](pathStr)(schemaFor, decoder, sks, concurrent)
  }
}
