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

sealed abstract private[saver] class AbstractCsvSaver[F[_], A](
  rdd: RDD[A],
  encoder: RowEncoder[A],
  csvConfiguration: CsvConfiguration,
  constraint: TypedEncoder[A],
  cfg: SaverConfig)
    extends AbstractSaver[F, A](cfg) {
  implicit private val enc: RowEncoder[A]  = encoder
  implicit private val te: TypedEncoder[A] = constraint

  def updateCsvConfig(f: CsvConfiguration => CsvConfiguration): AbstractCsvSaver[F, A]
  def overwrite: AbstractCsvSaver[F, A]
  def errorIfExists: AbstractCsvSaver[F, A]
  def single: AbstractCsvSaver[F, A]
  def multi: AbstractCsvSaver[F, A]

  final override protected def writeSingleFile(
    rdd: RDD[A],
    outPath: String,
    blocker: Blocker)(implicit ss: SparkSession, F: Concurrent[F], cs: ContextShift[F]): F[Unit] =
    rdd.stream[F].through(fileSink[F](blocker).csv(outPath, csvConfiguration)).compile.drain

  final override protected def writeMultiFiles(
    rdd: RDD[A],
    outPath: String,
    ss: SparkSession): Unit =
    TypedDataset
      .create(rdd)(constraint, ss)
      .write
      .option("sep", csvConfiguration.cellSeparator.toString)
      .option("header", csvConfiguration.hasHeader)
      .option("quote", csvConfiguration.quote.toString)
      .option("charset", "UTF8")
      .csv(outPath)

  final override protected def toDataFrame(rdd: RDD[A])(implicit ss: SparkSession): DataFrame =
    TypedDataset.create(rdd).dataset.toDF()

}

final class CsvSaver[F[_], A](
  rdd: RDD[A],
  encoder: RowEncoder[A],
  csvConfiguration: CsvConfiguration,
  constraint: TypedEncoder[A],
  cfg: SaverConfig)
    extends AbstractCsvSaver[F, A](rdd, encoder, csvConfiguration, constraint, cfg) {

  override def updateCsvConfig(f: CsvConfiguration => CsvConfiguration): CsvSaver[F, A] =
    new CsvSaver[F, A](rdd, encoder, f(csvConfiguration), constraint, cfg)

  private def mode(sm: SaveMode): CsvSaver[F, A] =
    new CsvSaver[F, A](rdd, encoder, csvConfiguration, constraint, cfg.withSaveMode(sm))

  override def overwrite: CsvSaver[F, A]     = mode(SaveMode.Overwrite)
  override def errorIfExists: CsvSaver[F, A] = mode(SaveMode.ErrorIfExists)

  override def single: CsvSaver[F, A] =
    new CsvSaver[F, A](rdd, encoder, csvConfiguration, constraint, cfg.withSingle)

  override def multi: CsvSaver[F, A] =
    new CsvSaver[F, A](rdd, encoder, csvConfiguration, constraint, cfg.withMulti)

  override def run(
    blocker: Blocker)(implicit ss: SparkSession, F: Concurrent[F], cs: ContextShift[F]): F[Unit] =
    saveRdd(rdd, params.outPath, blocker)
}

final class CsvPartitionSaver[F[_], A, K: ClassTag: Eq](
  rdd: RDD[A],
  encoder: RowEncoder[A],
  csvConfiguration: CsvConfiguration,
  constraint: TypedEncoder[A],
  cfg: SaverConfig,
  bucketing: A => K,
  pathBuilder: K => String
) extends AbstractCsvSaver[F, A](rdd, encoder, csvConfiguration, constraint, cfg) {

  override def updateCsvConfig(
    f: CsvConfiguration => CsvConfiguration): CsvPartitionSaver[F, A, K] =
    new CsvPartitionSaver[F, A, K](
      rdd,
      encoder,
      f(csvConfiguration),
      constraint,
      cfg,
      bucketing,
      pathBuilder)

  override def overwrite: CsvPartitionSaver[F, A, K] =
    new CsvPartitionSaver[F, A, K](
      rdd,
      encoder,
      csvConfiguration,
      constraint,
      cfg.withSaveMode(SaveMode.Overwrite),
      bucketing,
      pathBuilder)

  override def errorIfExists: CsvPartitionSaver[F, A, K] =
    new CsvPartitionSaver[F, A, K](
      rdd,
      encoder,
      csvConfiguration,
      constraint,
      cfg.withSaveMode(SaveMode.ErrorIfExists),
      bucketing,
      pathBuilder)

  override def single: CsvPartitionSaver[F, A, K] =
    new CsvPartitionSaver[F, A, K](
      rdd,
      encoder,
      csvConfiguration,
      constraint,
      cfg.withSingle,
      bucketing,
      pathBuilder)

  override def multi: CsvPartitionSaver[F, A, K] =
    new CsvPartitionSaver[F, A, K](
      rdd,
      encoder,
      csvConfiguration,
      constraint,
      cfg.withMulti,
      bucketing,
      pathBuilder)

  override def run(
    blocker: Blocker)(implicit ss: SparkSession, F: Concurrent[F], cs: ContextShift[F]): F[Unit] =
    savePartitionedRdd(rdd, blocker, bucketing, pathBuilder)
}
