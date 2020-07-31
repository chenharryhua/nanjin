package com.github.chenharryhua.nanjin.spark.saver

import cats.effect.{Blocker, Concurrent, ContextShift}
import cats.implicits._
import com.github.chenharryhua.nanjin.spark.{fileSink, RddExt}
import com.sksamuel.avro4s.Encoder
import monocle.macros.Lenses
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

@Lenses final case class BinaryAvroSaver[A](
  rdd: RDD[A],
  encoder: Encoder[A],
  outPath: String,
  saveMode: SaveMode) {
      implicit private val enc: Encoder[A] = encoder


  def mode(sm: SaveMode): BinaryAvroSaver[A] =
    BinaryAvroSaver.saveMode.set(sm)(this)

  def overwrite: BinaryAvroSaver[A]     = mode(SaveMode.Overwrite)
  def errorIfExists: BinaryAvroSaver[A] = mode(SaveMode.ErrorIfExists)

  private def writeSingleFile[F[_]](
    blocker: Blocker)(implicit ss: SparkSession, F: Concurrent[F], cs: ContextShift[F]): F[Unit] =
    rdd.stream[F].through(fileSink[F](blocker).binAvro(outPath)).compile.drain

  def run[F[_]](
    blocker: Blocker)(implicit ss: SparkSession, F: Concurrent[F], cs: ContextShift[F]): F[Unit] =
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
}
