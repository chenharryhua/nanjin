package com.github.chenharryhua.nanjin.spark.saver

import cats.Parallel
import cats.effect.{Blocker, Concurrent, ContextShift}
import cats.kernel.Eq
import com.github.chenharryhua.nanjin.spark.{fileSink, RddExt}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import scalapb.GeneratedMessage

import scala.reflect.ClassTag

sealed abstract private[saver] class AbstractProtobufSaver[F[_], A](rdd: RDD[A], cfg: SaverConfig)(
  implicit enc: A <:< GeneratedMessage)
    extends AbstractSaver[F, A](cfg) {

  final override protected def writeSingleFile(
    rdd: RDD[A],
    outPath: String,
    blocker: Blocker)(implicit ss: SparkSession, F: Concurrent[F], cs: ContextShift[F]): F[Unit] =
    rdd.stream[F].through(fileSink[F](blocker).protobuf[A](outPath)).compile.drain

}

final class ProtobufSaver[F[_], A](rdd: RDD[A], cfg: SaverConfig, outPath: String)(implicit
  enc: A <:< GeneratedMessage)
    extends AbstractProtobufSaver[F, A](rdd, cfg) {

  def run(
    blocker: Blocker)(implicit ss: SparkSession, F: Concurrent[F], cs: ContextShift[F]): F[Unit] =
    saveRdd(rdd, outPath, blocker)

  override def overwrite: ProtobufSaver[F, A] =
    new ProtobufSaver[F, A](rdd, cfg.withSaveMode(SaveMode.Overwrite), outPath)

  override def errorIfExists: ProtobufSaver[F, A] =
    new ProtobufSaver[F, A](rdd, cfg.withSaveMode(SaveMode.ErrorIfExists), outPath)

  override def ignoreIfExists: ProtobufSaver[F, A] =
    new ProtobufSaver[F, A](rdd, cfg.withSaveMode(SaveMode.Ignore), outPath)
}

final class ProtobufPartitionSaver[F[_], A, K: ClassTag: Eq](
  rdd: RDD[A],
  cfg: SaverConfig,
  bucketing: A => K,
  pathBuilder: K => String)(implicit enc: A <:< GeneratedMessage)
    extends AbstractProtobufSaver[F, A](rdd, cfg) with Partition[F, A, K] {

  override def overwrite: ProtobufPartitionSaver[F, A, K] =
    new ProtobufPartitionSaver[F, A, K](
      rdd,
      cfg.withSaveMode(SaveMode.Overwrite),
      bucketing,
      pathBuilder)

  override def errorIfExists: ProtobufPartitionSaver[F, A, K] =
    new ProtobufPartitionSaver[F, A, K](
      rdd,
      cfg.withSaveMode(SaveMode.ErrorIfExists),
      bucketing,
      pathBuilder)

  override def ignoreIfExists: ProtobufPartitionSaver[F, A, K] =
    new ProtobufPartitionSaver[F, A, K](
      rdd,
      cfg.withSaveMode(SaveMode.Ignore),
      bucketing,
      pathBuilder)

  override def reBucket[K1: ClassTag: Eq](
    bucketing: A => K1,
    pathBuilder: K1 => String): ProtobufPartitionSaver[F, A, K1] =
    new ProtobufPartitionSaver[F, A, K1](rdd, cfg, bucketing, pathBuilder)

  override def rePath(pathBuilder: K => String): ProtobufPartitionSaver[F, A, K] =
    new ProtobufPartitionSaver[F, A, K](rdd, cfg, bucketing, pathBuilder)

  override def parallel(num: Long): ProtobufPartitionSaver[F, A, K] =
    new ProtobufPartitionSaver[F, A, K](rdd, cfg.withParallism(num), bucketing, pathBuilder)

  override def run(blocker: Blocker)(implicit
    ss: SparkSession,
    F: Concurrent[F],
    cs: ContextShift[F],
    P: Parallel[F]): F[Unit] =
    savePartitionedRdd(rdd, blocker, bucketing, pathBuilder)
}
