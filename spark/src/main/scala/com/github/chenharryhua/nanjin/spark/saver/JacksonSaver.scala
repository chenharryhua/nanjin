package com.github.chenharryhua.nanjin.spark.saver

import cats.effect.{Blocker, Concurrent, ContextShift}
import cats.implicits._
import com.github.chenharryhua.nanjin.spark.mapreduce.NJJacksonKeyOutputFormat
import com.github.chenharryhua.nanjin.spark.{fileSink, RddExt}
import com.sksamuel.avro4s.Encoder
import monocle.macros.Lenses
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

@Lenses final case class JacksonSaver[F[_], A](
  rdd: RDD[A],
  encoder: Encoder[A],
  outPath: String,
  saveMode: SaveMode,
  singleOrMulti: SingleOrMulti) {

  implicit private val enc: Encoder[A] = encoder

  def mode(sm: SaveMode): JacksonSaver[F, A] =
    JacksonSaver.saveMode.set(sm)(this)

  def overwrite: JacksonSaver[F, A]     = mode(SaveMode.Overwrite)
  def errorIfExists: JacksonSaver[F, A] = mode(SaveMode.ErrorIfExists)

  def single: JacksonSaver[F, A] =
    JacksonSaver.singleOrMulti.set(SingleOrMulti.Single)(this)

  def multi: JacksonSaver[F, A] =
    JacksonSaver.singleOrMulti.set(SingleOrMulti.Multi)(this)

  private def writeSingleFile(
    blocker: Blocker)(implicit ss: SparkSession, F: Concurrent[F], cs: ContextShift[F]): F[Unit] =
    rdd.stream[F].through(fileSink[F](blocker).jackson(outPath)).compile.drain

  private def writeMultiFiles(ss: SparkSession): Unit =
    utils
      .genericRecordPair(rdd, encoder, ss)
      .saveAsNewAPIHadoopFile[NJJacksonKeyOutputFormat](outPath)

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
