package com.github.chenharryhua.nanjin.spark.saver

import cats.effect.{Blocker, Concurrent, ContextShift}
import cats.implicits._
import com.github.chenharryhua.nanjin.spark.{fileSink, RddExt}
import frameless.{TypedDataset, TypedEncoder}
import kantan.csv.{CsvConfiguration, RowEncoder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

final class CsvSaver[F[_], A](
  rdd: RDD[A],
  encoder: RowEncoder[A],
  csvConfiguration: CsvConfiguration,
  outPath: String,
  constraint: TypedEncoder[A],
  cfg: SaverConfig)
    extends Serializable {
  implicit private val enc: RowEncoder[A] = encoder

  val params: SaverParams = cfg.evalConfig

  def updateCsvConfig(f: CsvConfiguration => CsvConfiguration): CsvSaver[F, A] =
    new CsvSaver[F, A](rdd, encoder, f(csvConfiguration), outPath, constraint, cfg)

  def mode(sm: SaveMode): CsvSaver[F, A] =
    new CsvSaver[F, A](rdd, encoder, csvConfiguration, outPath, constraint, cfg.withSaveMode(sm))

  def overwrite: CsvSaver[F, A]     = mode(SaveMode.Overwrite)
  def errorIfExists: CsvSaver[F, A] = mode(SaveMode.ErrorIfExists)

  def single: CsvSaver[F, A] =
    new CsvSaver[F, A](rdd, encoder, csvConfiguration, outPath, constraint, cfg.withSingle)

  def multi: CsvSaver[F, A] =
    new CsvSaver[F, A](rdd, encoder, csvConfiguration, outPath, constraint, cfg.withMulti)

  private def writeSingleFile(
    blocker: Blocker)(implicit ss: SparkSession, F: Concurrent[F], cs: ContextShift[F]): F[Unit] =
    rdd.stream[F].through(fileSink[F](blocker).csv(outPath, csvConfiguration)).compile.drain

  private def writeMultiFiles(ss: SparkSession): Unit =
    TypedDataset
      .create(rdd)(constraint, ss)
      .write
      .option("sep", csvConfiguration.cellSeparator.toString)
      .option("header", csvConfiguration.hasHeader)
      .option("quote", csvConfiguration.quote.toString)
      .option("charset", "UTF8")
      .csv(outPath)

  def run(
    blocker: Blocker)(implicit ss: SparkSession, F: Concurrent[F], cs: ContextShift[F]): F[Unit] =
    params.singleOrMulti match {
      case SingleOrMulti.Single =>
        params.saveMode match {
          case SaveMode.Append => F.raiseError(new Exception("append mode is not support"))
          case SaveMode.Overwrite =>
            fileSink[F](blocker).delete(outPath) >> writeSingleFile(blocker)

          case SaveMode.ErrorIfExists =>
            fileSink[F](blocker).isExist(outPath).flatMap {
              case true  => F.raiseError(new Exception(s"$outPath already exist"))
              case false => writeSingleFile(blocker)
            }
          case SaveMode.Ignore =>
            fileSink[F](blocker).isExist(outPath).flatMap {
              case true  => F.pure(())
              case false => writeSingleFile(blocker)
            }
        }

      case SingleOrMulti.Multi =>
        params.saveMode match {
          case SaveMode.Append => F.raiseError(new Exception("append mode is not support"))
          case SaveMode.Overwrite =>
            fileSink[F](blocker).delete(outPath) >> F.delay(writeMultiFiles(ss))
          case SaveMode.ErrorIfExists =>
            fileSink[F](blocker).isExist(outPath).flatMap {
              case true  => F.raiseError(new Exception(s"$outPath already exist"))
              case false => F.delay(writeMultiFiles(ss))
            }
          case SaveMode.Ignore =>
            fileSink[F](blocker).isExist(outPath).flatMap {
              case true  => F.pure(())
              case false => F.delay(writeMultiFiles(ss))
            }
        }
    }
}
