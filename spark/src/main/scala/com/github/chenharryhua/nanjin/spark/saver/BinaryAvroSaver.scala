package com.github.chenharryhua.nanjin.spark.saver

import cats.effect.{Blocker, Concurrent, ContextShift}
import cats.implicits._
import com.github.chenharryhua.nanjin.spark.{fileSink, RddExt}
import com.sksamuel.avro4s.Encoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

final class BinaryAvroSaver[F[_], A](
  rdd: RDD[A],
  encoder: Encoder[A],
  outPath: String,
  cfg: SaverConfig) {
  implicit private val enc: Encoder[A] = encoder

  val params: SaverParams = cfg.evalConfig

  def mode(sm: SaveMode): BinaryAvroSaver[F, A] =
    new BinaryAvroSaver(rdd, encoder, outPath, cfg.withSaveMode(sm))

  def overwrite: BinaryAvroSaver[F, A]     = mode(SaveMode.Overwrite)
  def errorIfExists: BinaryAvroSaver[F, A] = mode(SaveMode.ErrorIfExists)

  private def writeSingleFile(
    blocker: Blocker)(implicit ss: SparkSession, F: Concurrent[F], cs: ContextShift[F]): F[Unit] =
    rdd.stream[F].through(fileSink[F](blocker).binAvro(outPath)).compile.drain

  def run(
    blocker: Blocker)(implicit ss: SparkSession, F: Concurrent[F], cs: ContextShift[F]): F[Unit] =
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
}
