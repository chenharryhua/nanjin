package com.github.chenharryhua.nanjin.spark

import cats.effect.{Concurrent, ContextShift}
import cats.implicits._
import com.sksamuel.avro4s._
import frameless.TypedDataset
import frameless.cats.implicits._
import fs2.Stream
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

private[spark] trait DatasetExtensions {

  implicit class TypedDatasetExt[A](private val tds: TypedDataset[A]) {

    def stream[F[_]: Concurrent]: Stream[F, A] =
      Stream.force(
        tds.toLocalIterator.map(it => Stream.fromIterator[F](it.asScala.flatMap(Option(_)))))

    def saveJackson[F[_]: ContextShift: Concurrent](pathStr: String)(
      implicit
      sparkSession: SparkSession,
      schemaFor: SchemaFor[A],
      encoder: Encoder[A]): Stream[F, Unit] =
      tds.stream[F].through(jacksonFileSink[F, A](pathStr))

    def saveAvro[F[_]: ContextShift: Concurrent](pathStr: String)(
      implicit
      sparkSession: SparkSession,
      schemaFor: SchemaFor[A],
      encoder: Encoder[A]): Stream[F, Unit] =
      tds.stream[F].through(avroFileSink[F, A](pathStr))
  }

  implicit class SparkSessionExt(private val sks: SparkSession) {

    implicit private val imp: SparkSession = sks

    def loadAvro[F[_]: ContextShift: Concurrent, A: Decoder: SchemaFor](
      pathStr: String): Stream[F, A] =
      avroFileSource[F, A](pathStr)

    def loadJackson[F[_]: ContextShift: Concurrent, A: Decoder: SchemaFor](
      pathStr: String): Stream[F, A] =
      jacksonFileSource[F, A](pathStr)
  }
}
