package com.github.chenharryhua.nanjin.spark.saver

import cats.Show
import cats.effect.{Blocker, Concurrent, ContextShift}
import cats.implicits._
import cats.kernel.Eq
import com.github.chenharryhua.nanjin.spark.{fileSink, RddExt}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.reflect.ClassTag

sealed abstract private[saver] class AbstractTextSaver[F[_], A](
  rdd: RDD[A],
  encoder: Show[A],
  cfg: SaverConfig)
    extends AbstractSaver[F, A](cfg) {
  implicit private val enc: Show[A] = encoder

  def overwrite: AbstractTextSaver[F, A]
  def errorIfExists: AbstractTextSaver[F, A]
  def single: AbstractTextSaver[F, A]
  def multi: AbstractTextSaver[F, A]

  final override protected def writeSingleFile(
    rdd: RDD[A],
    outPath: String,
    blocker: Blocker)(implicit ss: SparkSession, F: Concurrent[F], cs: ContextShift[F]): F[Unit] =
    rdd.stream[F].through(fileSink[F](blocker).text(outPath)).compile.drain

  final override protected def writeMultiFiles(
    rdd: RDD[A],
    outPath: String,
    ss: SparkSession): Unit =
    rdd.map(encoder.show).saveAsTextFile(outPath)

}

final class TextSaver[F[_], A](rdd: RDD[A], encoder: Show[A], cfg: SaverConfig)
    extends AbstractTextSaver[F, A](rdd, encoder, cfg) {

  private def mode(sm: SaveMode): TextSaver[F, A] =
    new TextSaver[F, A](rdd, encoder, cfg.withSaveMode(sm))

  override def overwrite: TextSaver[F, A]     = mode(SaveMode.Overwrite)
  override def errorIfExists: TextSaver[F, A] = mode(SaveMode.ErrorIfExists)

  override def single: TextSaver[F, A] =
    new TextSaver[F, A](rdd, encoder, cfg.withSingle)

  override def multi: TextSaver[F, A] =
    new TextSaver[F, A](rdd, encoder, cfg.withMulti)

  override def run(
    blocker: Blocker)(implicit ss: SparkSession, F: Concurrent[F], cs: ContextShift[F]): F[Unit] =
    saveRdd(rdd, params.outPath, blocker)
}

final class TextPartitionSaver[F[_], A, K: ClassTag: Eq](
  rdd: RDD[A],
  encoder: Show[A],
  cfg: SaverConfig,
  bucketing: A => K,
  pathBuilder: K => String)
    extends AbstractTextSaver[F, A](rdd, encoder, cfg) {

  override def overwrite: TextPartitionSaver[F, A, K] =
    new TextPartitionSaver[F, A, K](
      rdd,
      encoder,
      cfg.withSaveMode(SaveMode.Overwrite),
      bucketing,
      pathBuilder)

  override def errorIfExists: TextPartitionSaver[F, A, K] =
    new TextPartitionSaver[F, A, K](
      rdd,
      encoder,
      cfg.withSaveMode(SaveMode.ErrorIfExists),
      bucketing,
      pathBuilder)

  override def single: TextPartitionSaver[F, A, K] =
    new TextPartitionSaver[F, A, K](rdd, encoder, cfg.withSingle, bucketing, pathBuilder)

  override def multi: TextPartitionSaver[F, A, K] =
    new TextPartitionSaver[F, A, K](rdd, encoder, cfg.withMulti, bucketing, pathBuilder)

  override def run(
    blocker: Blocker)(implicit ss: SparkSession, F: Concurrent[F], cs: ContextShift[F]): F[Unit] =
    savePartitionedRdd(rdd, blocker, bucketing, pathBuilder)
}
