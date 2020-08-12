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

sealed abstract private[saver] class AbstractBinaryAvroSaver[F[_], A](
  rdd: RDD[A],
  encoder: Encoder[A],
  cfg: SaverConfig)
    extends AbstractSaver[F, A](cfg) {
  implicit private val enc: Encoder[A] = encoder

  def overwrite: AbstractBinaryAvroSaver[F, A]
  def errorIfExists: AbstractBinaryAvroSaver[F, A]

  final override protected def writeSingleFile(
    rdd: RDD[A],
    outPath: String,
    blocker: Blocker)(implicit ss: SparkSession, F: Concurrent[F], cs: ContextShift[F]): F[Unit] =
    rdd.stream[F].through(fileSink[F](blocker).binAvro(outPath)).compile.drain

  final override protected def toDataFrame(rdd: RDD[A])(implicit ss: SparkSession): DataFrame =
    rdd.toDF
}

final class BinaryAvroSaver[F[_], A](rdd: RDD[A], encoder: Encoder[A], cfg: SaverConfig)
    extends AbstractBinaryAvroSaver[F, A](rdd, encoder, cfg) {

  private def mode(sm: SaveMode): BinaryAvroSaver[F, A] =
    new BinaryAvroSaver(rdd, encoder, cfg.withSaveMode(sm))

  override def overwrite: BinaryAvroSaver[F, A]     = mode(SaveMode.Overwrite)
  override def errorIfExists: BinaryAvroSaver[F, A] = mode(SaveMode.ErrorIfExists)

  def run(
    blocker: Blocker)(implicit ss: SparkSession, F: Concurrent[F], cs: ContextShift[F]): F[Unit] =
    saveRdd(rdd, params.outPath, blocker)
}

final class BinaryAvroPartitionSaver[F[_], A, K: ClassTag: Eq](
  rdd: RDD[A],
  encoder: Encoder[A],
  cfg: SaverConfig,
  bucketing: A => K,
  pathBuilder: K => String)
    extends AbstractBinaryAvroSaver[F, A](rdd, encoder, cfg) with Partition[F, A, K] {

  private def mode(sm: SaveMode): BinaryAvroPartitionSaver[F, A, K] =
    new BinaryAvroPartitionSaver(rdd, encoder, cfg.withSaveMode(sm), bucketing, pathBuilder)

  override def overwrite: BinaryAvroPartitionSaver[F, A, K]     = mode(SaveMode.Overwrite)
  override def errorIfExists: BinaryAvroPartitionSaver[F, A, K] = mode(SaveMode.ErrorIfExists)

  override def reBucket[K1: ClassTag: Eq](
    bucketing: A => K1,
    pathBuilder: K1 => String): BinaryAvroPartitionSaver[F, A, K1] =
    new BinaryAvroPartitionSaver[F, A, K1](rdd, encoder, cfg, bucketing, pathBuilder)

  override def rePath(pathBuilder: K => String): BinaryAvroPartitionSaver[F, A, K] =
    new BinaryAvroPartitionSaver[F, A, K](rdd, encoder, cfg, bucketing, pathBuilder)

  override def parallel(num: Long): BinaryAvroPartitionSaver[F, A, K] =
    new BinaryAvroPartitionSaver[F, A, K](
      rdd,
      encoder,
      cfg.withParallism(num),
      bucketing,
      pathBuilder)

  override def run(blocker: Blocker)(implicit
    ss: SparkSession,
    F: Concurrent[F],
    CS: ContextShift[F],
    P: Parallel[F]): F[Unit] =
    savePartitionedRdd(rdd, blocker, bucketing, pathBuilder)
}
