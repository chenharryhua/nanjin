package com.github.chenharryhua.nanjin.spark.saver

import cats.effect.{Blocker, Concurrent, ContextShift}
import cats.implicits._
import com.github.chenharryhua.nanjin.spark.{fileSink, RddExt}
import io.circe.Encoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

final class CirceJsonSaver[F[_], A](
  rdd: RDD[A],
  encoder: Encoder[A],
  outPath: String,
  cfg: SaverConfig) {

  implicit private val enc: Encoder[A] = encoder

  val params: SaverParams = cfg.evalConfig

  def mode(sm: SaveMode): CirceJsonSaver[F, A] =
    new CirceJsonSaver[F, A](rdd, encoder, outPath, cfg.withSaveMode(sm))

  def overwrite: CirceJsonSaver[F, A]     = mode(SaveMode.Overwrite)
  def errorIfExists: CirceJsonSaver[F, A] = mode(SaveMode.ErrorIfExists)

  def single: CirceJsonSaver[F, A] =
    new CirceJsonSaver[F, A](rdd, encoder, outPath, cfg.withSingle)

  def multi: CirceJsonSaver[F, A] =
    new CirceJsonSaver[F, A](rdd, encoder, outPath, cfg.withMulti)

  private def writeSingleFile(
    blocker: Blocker)(implicit ss: SparkSession, F: Concurrent[F], cs: ContextShift[F]): F[Unit] =
    rdd.stream[F].through(fileSink[F](blocker).circe(outPath)).compile.drain

  private def writeMultiFiles(ss: SparkSession): Unit =
    rdd.map(encoder(_).noSpaces).saveAsTextFile(outPath)

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
