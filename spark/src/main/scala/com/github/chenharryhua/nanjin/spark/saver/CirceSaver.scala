package com.github.chenharryhua.nanjin.spark.saver

import cats.effect.{Blocker, Concurrent, ContextShift}
import cats.implicits._
import cats.kernel.Eq
import com.github.chenharryhua.nanjin.spark.{fileSink, RddExt}
import io.circe.Encoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.reflect.ClassTag

sealed abstract private[saver] class AbstractCirceSaver[F[_], A](
  rdd: RDD[A],
  encoder: Encoder[A],
  cfg: SaverConfig)
    extends AbstractSaver[F, A](cfg) {
  implicit private val enc: Encoder[A] = encoder

  def overwrite: AbstractCirceSaver[F, A]
  def errorIfExists: AbstractCirceSaver[F, A]
  def single: AbstractCirceSaver[F, A]
  def multi: AbstractCirceSaver[F, A]

  final override protected def writeSingleFile(
    rdd: RDD[A],
    outPath: String,
    blocker: Blocker)(implicit ss: SparkSession, F: Concurrent[F], cs: ContextShift[F]): F[Unit] =
    rdd.stream[F].through(fileSink[F](blocker).circe(outPath)).compile.drain

  final override protected def writeMultiFiles(
    rdd: RDD[A],
    outPath: String,
    ss: SparkSession): Unit =
    rdd.map(encoder(_).noSpaces).saveAsTextFile(outPath)
}

final class CirceSaver[F[_], A](rdd: RDD[A], encoder: Encoder[A], cfg: SaverConfig)
    extends AbstractCirceSaver[F, A](rdd, encoder, cfg) {

  private def mode(sm: SaveMode): CirceSaver[F, A] =
    new CirceSaver[F, A](rdd, encoder, cfg.withSaveMode(sm))

  override def overwrite: CirceSaver[F, A]     = mode(SaveMode.Overwrite)
  override def errorIfExists: CirceSaver[F, A] = mode(SaveMode.ErrorIfExists)

  override def single: CirceSaver[F, A] =
    new CirceSaver[F, A](rdd, encoder, cfg.withSingle)

  override def multi: CirceSaver[F, A] =
    new CirceSaver[F, A](rdd, encoder, cfg.withMulti)

  override def run(
    blocker: Blocker)(implicit ss: SparkSession, F: Concurrent[F], cs: ContextShift[F]): F[Unit] =
    saveRdd(rdd, params.outPath, blocker)
}

final class CircePartitionSaver[F[_], A, K: ClassTag: Eq](
  rdd: RDD[A],
  encoder: Encoder[A],
  cfg: SaverConfig,
  bucketing: A => K,
  pathBuilder: K => String)
    extends AbstractCirceSaver[F, A](rdd, encoder, cfg) {

  private def mode(sm: SaveMode): CircePartitionSaver[F, A, K] =
    new CircePartitionSaver[F, A, K](rdd, encoder, cfg.withSaveMode(sm), bucketing, pathBuilder)

  override def overwrite: CircePartitionSaver[F, A, K]     = mode(SaveMode.Overwrite)
  override def errorIfExists: CircePartitionSaver[F, A, K] = mode(SaveMode.ErrorIfExists)

  override def multi: CircePartitionSaver[F, A, K] =
    new CircePartitionSaver[F, A, K](rdd, encoder, cfg.withMulti, bucketing, pathBuilder)

  override def single: CircePartitionSaver[F, A, K] =
    new CircePartitionSaver[F, A,K](rdd, encoder, cfg.withSingle, bucketing, pathBuilder)

  override def run(
    blocker: Blocker)(implicit ss: SparkSession, F: Concurrent[F], cs: ContextShift[F]): F[Unit] =
    savePartitionedRdd(rdd, blocker, bucketing, pathBuilder)

}
