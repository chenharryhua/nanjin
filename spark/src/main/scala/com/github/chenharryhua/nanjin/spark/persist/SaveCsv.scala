package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.{Blocker, Concurrent, ContextShift}
import cats.{Eq, Parallel}
import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.spark.{AvroTypedEncoder, RddExt}
import kantan.csv.{CsvConfiguration, RowEncoder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.reflect.ClassTag

final class SaveCsv[F[_], A](
  rdd: RDD[A],
  ate: AvroTypedEncoder[A],
  csvConfiguration: CsvConfiguration,
  cfg: HoarderConfig)(implicit rowEncoder: RowEncoder[A])
    extends Serializable {
  implicit private val tag: ClassTag[A] = ate.classTag

  val params: HoarderParams = cfg.evalConfig

  def updateCsvConfig(f: CsvConfiguration => CsvConfiguration): SaveCsv[F, A] =
    new SaveCsv[F, A](rdd, ate, f(csvConfiguration), cfg)

  private def updateConfig(cfg: HoarderConfig): SaveCsv[F, A] =
    new SaveCsv[F, A](rdd, ate, csvConfiguration, cfg)

  def file: SaveCsv[F, A]   = updateConfig(cfg.withSingleFile)
  def folder: SaveCsv[F, A] = updateConfig(cfg.withFolder)

  def run(
    blocker: Blocker)(implicit F: Concurrent[F], cs: ContextShift[F], ss: SparkSession): F[Unit] = {
    val sma: SaveModeAware[F] = new SaveModeAware[F](params.saveMode, params.outPath, ss)
    params.folderOrFile match {
      case FolderOrFile.SingleFile =>
        sma.checkAndRun(blocker)(
          rdd
            .map(ate.avroCodec.idConversion)
            .stream[F]
            .through(fileSink[F](blocker).csv(params.outPath, csvConfiguration))
            .compile
            .drain)
      case FolderOrFile.Folder =>
        val csv = F.delay(
          ate
            .normalize(rdd)
            .write
            .mode(SaveMode.Overwrite)
            .option("sep", csvConfiguration.cellSeparator.toString)
            .option("header", csvConfiguration.hasHeader)
            .option("quote", csvConfiguration.quote.toString)
            .option("charset", "UTF8")
            .csv(params.outPath))
        sma.checkAndRun(blocker)(csv)
    }
  }
}

final class PartitionCsv[F[_], A, K: ClassTag: Eq](
  rdd: RDD[A],
  ate: AvroTypedEncoder[A],
  csvConfiguration: CsvConfiguration,
  cfg: HoarderConfig,
  bucketing: A => Option[K],
  pathBuilder: (NJFileFormat, K) => String)(implicit rowEncoder: RowEncoder[A])
    extends AbstractPartition[F, A, K] {
  implicit private val tag: ClassTag[A] = ate.classTag

  val params: HoarderParams = cfg.evalConfig

  def updateCsvConfig(f: CsvConfiguration => CsvConfiguration): PartitionCsv[F, A, K] =
    new PartitionCsv[F, A, K](rdd, ate, f(csvConfiguration), cfg, bucketing, pathBuilder)

  def run(blocker: Blocker)(implicit
    F: Concurrent[F],
    CS: ContextShift[F],
    P: Parallel[F],
    ss: SparkSession): F[Unit] =
    savePartition(
      blocker,
      rdd,
      params.parallelism,
      params.format,
      bucketing,
      pathBuilder,
      (r, p) => new SaveCsv[F, A](r, ate, csvConfiguration, cfg.withOutPutPath(p)).run(blocker))
}
