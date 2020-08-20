package com.github.chenharryhua.nanjin.spark.saver

import cats.Parallel
import cats.effect.{Blocker, Concurrent, ContextShift}
import cats.implicits._
import cats.kernel.Eq
import com.github.chenharryhua.nanjin.spark.{fileSink, RddExt}
import frameless.{TypedDataset, TypedEncoder}
import kantan.csv.{CsvConfiguration, RowEncoder}
import monocle.Lens
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.reflect.ClassTag

sealed abstract private[saver] class AbstractCsvSaver[F[_], A](
  encoder: RowEncoder[A],
  csvConfiguration: CsvConfiguration,
  constraint: TypedEncoder[A])
    extends AbstractSaver[F, A] {
  implicit private val enc: RowEncoder[A]  = encoder
  implicit private val te: TypedEncoder[A] = constraint

  def updateCsvConfig(f: CsvConfiguration => CsvConfiguration): AbstractCsvSaver[F, A]
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
  outPath: String,
  cfg: SaverConfig)
    extends AbstractCsvSaver[F, A](encoder, csvConfiguration, constraint) {

  override def updateCsvConfig(f: CsvConfiguration => CsvConfiguration): CsvSaver[F, A] =
    new CsvSaver[F, A](rdd, encoder, f(csvConfiguration), constraint, outPath, cfg)

  override def updateConfig(cfg: SaverConfig): CsvSaver[F, A] =
    new CsvSaver[F, A](rdd, encoder, csvConfiguration, constraint, outPath, cfg)

  override def errorIfExists: CsvSaver[F, A]  = updateConfig(cfg.withError)
  override def overwrite: CsvSaver[F, A]      = updateConfig(cfg.withOverwrite)
  override def ignoreIfExists: CsvSaver[F, A] = updateConfig(cfg.withIgnore)

  override def single: CsvSaver[F, A] = updateConfig(cfg.withSingle)
  override def multi: CsvSaver[F, A]  = updateConfig(cfg.withMulti)

  def run(
    blocker: Blocker)(implicit ss: SparkSession, F: Concurrent[F], cs: ContextShift[F]): F[Unit] =
    saveRdd(rdd, outPath, cfg.evalConfig, blocker)

}

final class CsvPartitionSaver[F[_], A, K: ClassTag: Eq](
  rdd: RDD[A],
  encoder: RowEncoder[A],
  csvConfiguration: CsvConfiguration,
  constraint: TypedEncoder[A],
  bucketing: A => Option[K],
  pathBuilder: K => String,
  val cfg: SaverConfig
) extends AbstractCsvSaver[F, A](encoder, csvConfiguration, constraint) with Partition[F, A, K] {

  override def updateCsvConfig(
    f: CsvConfiguration => CsvConfiguration): CsvPartitionSaver[F, A, K] =
    new CsvPartitionSaver[F, A, K](
      rdd,
      encoder,
      f(csvConfiguration),
      constraint,
      bucketing,
      pathBuilder,
      cfg)

  override def updateConfig(cfg: SaverConfig): CsvPartitionSaver[F, A, K] =
    new CsvPartitionSaver[F, A, K](
      rdd,
      encoder,
      csvConfiguration,
      constraint,
      bucketing,
      pathBuilder,
      cfg)

  override def errorIfExists: CsvPartitionSaver[F, A, K]  = updateConfig(cfg.withError)
  override def overwrite: CsvPartitionSaver[F, A, K]      = updateConfig(cfg.withOverwrite)
  override def ignoreIfExists: CsvPartitionSaver[F, A, K] = updateConfig(cfg.withIgnore)

  override def single: CsvPartitionSaver[F, A, K] = updateConfig(cfg.withSingle)
  override def multi: CsvPartitionSaver[F, A, K]  = updateConfig(cfg.withMulti)

  override def parallel(num: Long): CsvPartitionSaver[F, A, K] =
    updateConfig(cfg.withParallel(num))

  override def reBucket[K1: ClassTag: Eq](
    bucketing: A => Option[K1],
    pathBuilder: K1 => String): CsvPartitionSaver[F, A, K1] =
    new CsvPartitionSaver[F, A, K1](
      rdd,
      encoder,
      csvConfiguration,
      constraint,
      bucketing,
      pathBuilder,
      cfg)

  override def rePath(pathBuilder: K => String): CsvPartitionSaver[F, A, K] =
    new CsvPartitionSaver[F, A, K](
      rdd,
      encoder,
      csvConfiguration,
      constraint,
      bucketing,
      pathBuilder,
      cfg)

  override def run(blocker: Blocker)(implicit
    ss: SparkSession,
    F: Concurrent[F],
    cs: ContextShift[F],
    P: Parallel[F]): F[Unit] =
    savePartitionRdd(rdd, cfg.evalConfig, blocker, bucketing, pathBuilder)

}
