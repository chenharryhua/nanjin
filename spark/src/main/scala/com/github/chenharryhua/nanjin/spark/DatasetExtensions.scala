package com.github.chenharryhua.nanjin.spark

import cats.effect.{Concurrent, Sync}
import cats.implicits._
import com.github.chenharryhua.nanjin.kafka.Keyboard
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
          .eval(
            tds.toLocalIterator.map(it => Stream.fromIterator[F](it.asScala.flatMap(Option(_)))))
          .flatten
          .pauseWhen(kb.map(_.contains(Keyboard.pauSe)))
          .interruptWhen(kb.map(_.contains(Keyboard.Quit)))
      } yield data

    def saveParquet(path: String): Unit = tds.write.parquet(path)
    def saveCsv(path: String): Unit     = tds.write.csv(path)
    def saveJson(path: String): Unit    = tds.write.json(path)
    def saveAvro(path: String): Unit    = tds.write.format("avro").save(path)
  }

  implicit class NJSparkSessionExt(private val sparkSession: SparkSession) {

    def readParquet[A: TypedEncoder](path: String): TypedDataset[A] =
      TypedDataset.createUnsafe[A](sparkSession.read.parquet(path))

    def readCsv[A: TypedEncoder](
      path: String,
      params: FileFormat.Csv = FileFormat.Csv.default): TypedDataset[A] =
      TypedDataset.createUnsafe[A](sparkSession.read.options(params.options).csv(path))

    def readJson[A: TypedEncoder](
      path: String,
      params: FileFormat = FileFormat.Json): TypedDataset[A] =
      TypedDataset.createUnsafe[A](sparkSession.read.options(params.options).json(path))

    def readAvro[A: TypedEncoder](
      path: String,
      params: FileFormat = FileFormat.Avro): TypedDataset[A] =
      TypedDataset.createUnsafe[A](
        sparkSession.read.format("avro").options(params.options).load(path))
  }

  implicit final class SparkDataStreamWriterSyntax[A](dsw: DataStreamWriter[A]) {

    def run[F[_]](implicit F: Sync[F]): F[Unit] =
      F.bracket(F.delay(dsw.start))(s => F.delay(s.awaitTermination()))(_ => F.pure(()))
  }
}
