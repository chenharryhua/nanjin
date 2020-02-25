package com.github.chenharryhua.nanjin.spark

import cats.effect.{ContextShift, Sync}
import cats.implicits._
import com.sksamuel.avro4s._
import frameless.TypedDataset
import frameless.cats.implicits._
import fs2.Stream
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import com.github.chenharryhua.nanjin.pipes.sinks.{avroFileSink, jacksonFileSink}
import com.github.chenharryhua.nanjin.pipes.sources.{avroFileSource, jacksonFileSource}

import scala.collection.JavaConverters._

private[spark] trait DatasetExtensions {

  implicit class RddExt[A](private val rdd: RDD[A]) {

    def stream[F[_]: Sync]: Stream[F, A] =
      Stream.fromIterator(rdd.toLocalIterator)
  }

  implicit class TypedDatasetExt[A](private val tds: TypedDataset[A]) {

    def stream[F[_]: Sync]: Stream[F, A] =
      Stream.force(
        tds.toLocalIterator.map(it => Stream.fromIterator[F](it.asScala.flatMap(Option(_)))))

    def saveJackson[F[_]: ContextShift: Sync](pathStr: String)(
      implicit
      sparkSession: SparkSession,
      schemaFor: SchemaFor[A],
      encoder: Encoder[A]): Stream[F, Unit] =
      tds
        .stream[F]
        .through(jacksonFileSink[F, A](pathStr, sparkSession.sparkContext.hadoopConfiguration))

    def saveAvro[F[_]: ContextShift: Sync](pathStr: String)(
      implicit
      sparkSession: SparkSession,
      schemaFor: SchemaFor[A],
      encoder: Encoder[A]): Stream[F, Unit] =
      tds
        .stream[F]
        .through(avroFileSink[F, A](pathStr, sparkSession.sparkContext.hadoopConfiguration))
  }

  implicit class SparkSessionExt(private val sks: SparkSession) {

    implicit private val imp: SparkSession = sks

    def loadAvro[F[_]: ContextShift: Sync, A: Decoder: SchemaFor](pathStr: String): Stream[F, A] =
      avroFileSource[F, A](pathStr, sks.sparkContext.hadoopConfiguration)

    def loadJackson[F[_]: ContextShift: Sync, A: Decoder: SchemaFor](
      pathStr: String): Stream[F, A] =
      jacksonFileSource[F, A](pathStr, sks.sparkContext.hadoopConfiguration)
  }
}
