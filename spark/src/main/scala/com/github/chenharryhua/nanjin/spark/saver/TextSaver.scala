package com.github.chenharryhua.nanjin.spark.saver

import cats.Show
import cats.effect.{Blocker, Concurrent, ContextShift}
import cats.implicits._
import com.github.chenharryhua.nanjin.spark.{fileSink, RddExt}
import monocle.macros.Lenses
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

@Lenses final case class TextSaver[A](
  rdd: RDD[A],
  encoder: Show[A],
  outPath: String,
  saveMode: SaveMode,
  singleOrMulti: SingleOrMulti) {
  implicit private val enc: Show[A] = encoder

  def mode(sm: SaveMode): TextSaver[A] =
    TextSaver.saveMode.set(sm)(this)

  def overwrite: TextSaver[A]     = mode(SaveMode.Overwrite)
  def errorIfExists: TextSaver[A] = mode(SaveMode.ErrorIfExists)

  def single: TextSaver[A] =
    TextSaver.singleOrMulti.set(SingleOrMulti.Single)(this)

  def multi: TextSaver[A] =
    TextSaver.singleOrMulti.set(SingleOrMulti.Multi)(this)

  private def writeSingleFile[F[_]](
    blocker: Blocker)(implicit ss: SparkSession, F: Concurrent[F], cs: ContextShift[F]): F[Unit] =
    rdd.stream[F].through(fileSink[F](blocker).text(outPath)).compile.drain

  private def writeMultiFiles(ss: SparkSession): Unit =
    rdd.map(encoder.show).saveAsTextFile(outPath)

  def run[F[_]](
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
