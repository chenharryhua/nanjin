package com.github.chenharryhua.nanjin.spark.saver

import cats.effect.{Blocker, Concurrent, ContextShift}
import cats.implicits._
import cats.kernel.Eq
import cats.{Parallel, Show}
import com.github.chenharryhua.nanjin.spark.{fileSink, RddExt}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

sealed abstract private[saver] class AbstractTextSaver[F[_], A](encoder: Show[A])
    extends AbstractSaver[F, A] {
  implicit private val enc: Show[A] = encoder

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

final class TextSaver[F[_], A](rdd: RDD[A], encoder: Show[A], outPath: String, cfg: SaverConfig)
    extends AbstractTextSaver[F, A](encoder) {

  override def updateConfig(cfg: SaverConfig): TextSaver[F, A] =
    new TextSaver[F, A](rdd, encoder, outPath, cfg)

  override def errorIfExists: TextSaver[F, A]  = updateConfig(cfg.withError)
  override def overwrite: TextSaver[F, A]      = updateConfig(cfg.withOverwrite)
  override def ignoreIfExists: TextSaver[F, A] = updateConfig(cfg.withIgnore)

  override def single: TextSaver[F, A] = updateConfig(cfg.withSingle)
  override def multi: TextSaver[F, A]  = updateConfig(cfg.withMulti)

  def run(
    blocker: Blocker)(implicit ss: SparkSession, F: Concurrent[F], cs: ContextShift[F]): F[Unit] =
    saveRdd(rdd, outPath, cfg.evalConfig, blocker)
}

final class TextPartitionSaver[F[_], A, K: ClassTag: Eq](
  rdd: RDD[A],
  encoder: Show[A],
  bucketing: A => Option[K],
  pathBuilder: K => String,
  val cfg: SaverConfig)
    extends AbstractTextSaver[F, A](encoder) with Partition[F, A, K] {

  override def updateConfig(cfg: SaverConfig): TextPartitionSaver[F, A, K] =
    new TextPartitionSaver[F, A, K](rdd, encoder, bucketing, pathBuilder, cfg)

  override def errorIfExists: TextPartitionSaver[F, A, K]  = updateConfig(cfg.withError)
  override def overwrite: TextPartitionSaver[F, A, K]      = updateConfig(cfg.withOverwrite)
  override def ignoreIfExists: TextPartitionSaver[F, A, K] = updateConfig(cfg.withIgnore)

  override def single: TextPartitionSaver[F, A, K] = updateConfig(cfg.withSingle)
  override def multi: TextPartitionSaver[F, A, K]  = updateConfig(cfg.withMulti)

  override def parallel(num: Long): TextPartitionSaver[F, A, K] =
    updateConfig(cfg.withParallel(num))

  override def reBucket[K1: ClassTag: Eq](
    bucketing: A => Option[K1],
    pathBuilder: K1 => String): TextPartitionSaver[F, A, K1] =
    new TextPartitionSaver[F, A, K1](rdd, encoder, bucketing, pathBuilder, cfg)

  override def rePath(pathBuilder: K => String): TextPartitionSaver[F, A, K] =
    new TextPartitionSaver[F, A, K](rdd, encoder, bucketing, pathBuilder, cfg)

  override def run(blocker: Blocker)(implicit
    ss: SparkSession,
    F: Concurrent[F],
    cs: ContextShift[F],
    P: Parallel[F]): F[Unit] =
    savePartitionRdd(rdd, cfg.evalConfig, blocker, bucketing, pathBuilder)
}
