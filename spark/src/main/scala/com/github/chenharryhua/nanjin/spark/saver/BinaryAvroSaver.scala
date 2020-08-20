package com.github.chenharryhua.nanjin.spark.saver

import cats.Parallel
import cats.effect.{Blocker, Concurrent, ContextShift}
import cats.implicits._
import cats.kernel.Eq
import com.github.chenharryhua.nanjin.spark.{fileSink, RddExt}
import com.sksamuel.avro4s.Encoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.reflect.ClassTag

sealed abstract private[saver] class AbstractBinaryAvroSaver[F[_], A](encoder: Encoder[A])
    extends AbstractSaver[F, A] {
  implicit private val enc: Encoder[A] = encoder

  final override protected def writeSingleFile(
    rdd: RDD[A],
    outPath: String,
    blocker: Blocker)(implicit ss: SparkSession, F: Concurrent[F], cs: ContextShift[F]): F[Unit] =
    rdd.stream[F].through(fileSink[F](blocker).binAvro(outPath)).compile.drain

  final override protected def toDataFrame(rdd: RDD[A])(implicit ss: SparkSession): DataFrame =
    rdd.toDF
}

final class BinaryAvroSaver[F[_], A](
  rdd: RDD[A],
  encoder: Encoder[A],
  outPath: String,
  cfg: SaverConfig)
    extends AbstractBinaryAvroSaver[F, A](encoder) {

  override def updateConfig(cfg: SaverConfig): BinaryAvroSaver[F, A] =
    new BinaryAvroSaver(rdd, encoder, outPath, cfg)

  override def errorIfExists: BinaryAvroSaver[F, A]  = updateConfig(cfg.withError)
  override def overwrite: BinaryAvroSaver[F, A]      = updateConfig(cfg.withOverwrite)
  override def ignoreIfExists: BinaryAvroSaver[F, A] = updateConfig(cfg.withIgnore)

  def run(
    blocker: Blocker)(implicit ss: SparkSession, F: Concurrent[F], cs: ContextShift[F]): F[Unit] =
    saveRdd(rdd, outPath, cfg.evalConfig, blocker)

}

final class BinaryAvroPartitionSaver[F[_], A, K: ClassTag: Eq](
  rdd: RDD[A],
  encoder: Encoder[A],
  bucketing: A => Option[K],
  pathBuilder: K => String,
  val cfg: SaverConfig)
    extends AbstractBinaryAvroSaver[F, A](encoder) with Partition[F, A, K] {

  override def updateConfig(cfg: SaverConfig): BinaryAvroPartitionSaver[F, A, K] =
    new BinaryAvroPartitionSaver(rdd, encoder, bucketing, pathBuilder, cfg)

  override def errorIfExists: BinaryAvroPartitionSaver[F, A, K]  = updateConfig(cfg.withError)
  override def overwrite: BinaryAvroPartitionSaver[F, A, K]      = updateConfig(cfg.withOverwrite)
  override def ignoreIfExists: BinaryAvroPartitionSaver[F, A, K] = updateConfig(cfg.withIgnore)

  override def parallel(num: Long): BinaryAvroPartitionSaver[F, A, K] =
    updateConfig(cfg.withParallel(num))

  override def reBucket[K1: ClassTag: Eq](
    bucketing: A => Option[K1],
    pathBuilder: K1 => String): BinaryAvroPartitionSaver[F, A, K1] =
    new BinaryAvroPartitionSaver[F, A, K1](rdd, encoder, bucketing, pathBuilder, cfg)

  override def rePath(pathBuilder: K => String): BinaryAvroPartitionSaver[F, A, K] =
    new BinaryAvroPartitionSaver[F, A, K](rdd, encoder, bucketing, pathBuilder, cfg)

  override def run(blocker: Blocker)(implicit
    ss: SparkSession,
    F: Concurrent[F],
    CS: ContextShift[F],
    P: Parallel[F]): F[Unit] =
    savePartitionRdd(rdd, cfg.evalConfig, blocker, bucketing, pathBuilder)

}
