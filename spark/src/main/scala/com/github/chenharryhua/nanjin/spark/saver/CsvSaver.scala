package com.github.chenharryhua.nanjin.spark.saver

import cats.effect.{Blocker, Concurrent, ContextShift}
import cats.implicits._
import com.github.chenharryhua.nanjin.spark.{fileSink, RddExt}
import frameless.{TypedDataset, TypedEncoder}
import kantan.csv.{CsvConfiguration, RowEncoder}
import monocle.macros.Lenses
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

@Lenses final case class CsvSaver[F[_], A](
  rdd: RDD[A],
  encoder: RowEncoder[A],
  csvConfiguration: CsvConfiguration,
  outPath: String,
  saveMode: SaveMode,
  singleOrMulti: SingleOrMulti,
  constraint: TypedEncoder[A]) {
  implicit private val enc: RowEncoder[A] = encoder

  def updateCsvConfig(f: CsvConfiguration => CsvConfiguration): CsvSaver[F, A] =
    CsvSaver.csvConfiguration[F, A].modify(f)(this)

  def mode(sm: SaveMode): CsvSaver[F, A] =
    CsvSaver.saveMode.set(sm)(this)

  def overwrite: CsvSaver[F, A]     = mode(SaveMode.Overwrite)
  def errorIfExists: CsvSaver[F, A] = mode(SaveMode.ErrorIfExists)

  def single: CsvSaver[F, A] =
    CsvSaver.singleOrMulti.set(SingleOrMulti.Single)(this)

  def multi: CsvSaver[F, A] =
    CsvSaver.singleOrMulti.set(SingleOrMulti.Multi)(this)

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
    singleOrMulti match {
      case SingleOrMulti.Single =>
        saveMode match {
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
        saveMode match {
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
