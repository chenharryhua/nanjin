package com.github.chenharryhua.nanjin.spark.saver

import cats.effect.{Blocker, Concurrent, ContextShift}
import cats.implicits._
import cats.kernel.Eq
import com.github.chenharryhua.nanjin.spark.{fileSink, RddExt}
import frameless.{TypedDataset, TypedEncoder}
import kantan.csv.{CsvConfiguration, RowEncoder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.reflect.ClassTag

final class CsvSaver[F[_], A](
  rdd: RDD[A],
  encoder: RowEncoder[A],
  csvConfiguration: CsvConfiguration,
  constraint: TypedEncoder[A],
  cfg: SaverConfig)
    extends AbstractSaver[F, A](cfg) {
  implicit private val enc: RowEncoder[A]  = encoder
  implicit private val te: TypedEncoder[A] = constraint

  def updateCsvConfig(f: CsvConfiguration => CsvConfiguration): CsvSaver[F, A] =
    new CsvSaver[F, A](rdd, encoder, f(csvConfiguration), constraint, cfg)

  private def mode(sm: SaveMode): CsvSaver[F, A] =
    new CsvSaver[F, A](rdd, encoder, csvConfiguration, constraint, cfg.withSaveMode(sm))

  def overwrite: CsvSaver[F, A]     = mode(SaveMode.Overwrite)
  def errorIfExists: CsvSaver[F, A] = mode(SaveMode.ErrorIfExists)

  def single: CsvSaver[F, A] =
    new CsvSaver[F, A](rdd, encoder, csvConfiguration, constraint, cfg.withSingle)

  def multi: CsvSaver[F, A] =
    new CsvSaver[F, A](rdd, encoder, csvConfiguration, constraint, cfg.withMulti)

  override protected def writeSingleFile(rdd: RDD[A], outPath: String, blocker: Blocker)(implicit
    ss: SparkSession,
    F: Concurrent[F],
    cs: ContextShift[F]): F[Unit] =
    rdd.stream[F].through(fileSink[F](blocker).csv(outPath, csvConfiguration)).compile.drain

  override protected def writeMultiFiles(rdd: RDD[A], outPath: String, ss: SparkSession): Unit =
    TypedDataset
      .create(rdd)(constraint, ss)
      .write
      .option("sep", csvConfiguration.cellSeparator.toString)
      .option("header", csvConfiguration.hasHeader)
      .option("quote", csvConfiguration.quote.toString)
      .option("charset", "UTF8")
      .csv(outPath)

  override protected def toDataFrame(rdd: RDD[A])(implicit ss: SparkSession): DataFrame =
    TypedDataset.create(rdd).dataset.toDF()

  def run(
    blocker: Blocker)(implicit ss: SparkSession, F: Concurrent[F], cs: ContextShift[F]): F[Unit] =
    saveRdd(rdd, params.outPath, blocker)

  def runPartition[K: ClassTag: Eq](blocker: Blocker)(bucketing: A => K)(
    pathBuilder: K => String)(implicit
    F: Concurrent[F],
    ce: ContextShift[F],
    ss: SparkSession): F[Unit] =
    savePartitionedRdd(rdd, blocker, bucketing, pathBuilder)
}
