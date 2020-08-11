package com.github.chenharryhua.nanjin.spark.saver

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
  def overwrite: AbstractProtobufSaver[F, A]
  def errorIfExists: AbstractProtobufSaver[F, A]

  final override protected def writeSingleFile(
    rdd: RDD[A],
    outPath: String,
    blocker: Blocker)(implicit ss: SparkSession, F: Concurrent[F], cs: ContextShift[F]): F[Unit] =
    rdd.stream[F].through(fileSink[F](blocker).protobuf[A](outPath)).compile.drain

}

final class ProtobufSaver[F[_], A](rdd: RDD[A], cfg: SaverConfig)(implicit
  enc: A <:< GeneratedMessage)
    extends AbstractProtobufSaver[F, A](rdd, cfg) {

  override def run(
    blocker: Blocker)(implicit ss: SparkSession, F: Concurrent[F], cs: ContextShift[F]): F[Unit] =
    saveRdd(rdd, params.outPath, blocker)

  override def overwrite: ProtobufSaver[F, A] =
    new ProtobufSaver[F, A](rdd, cfg.withSaveMode(SaveMode.Overwrite))

  override def errorIfExists: ProtobufSaver[F, A] =
    new ProtobufSaver[F, A](rdd, cfg.withSaveMode(SaveMode.ErrorIfExists))
}

final class ProtobufPartitionSaver[F[_], A, K: ClassTag: Eq](
  rdd: RDD[A],
  cfg: SaverConfig,
  bucketing: A => K,
  pathBuilder: K => String)(implicit enc: A <:< GeneratedMessage)
    extends AbstractProtobufSaver[F, A](rdd, cfg) {

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

  def reBucket[K1: ClassTag: Eq](
    bucketing: A => K1,
    pathBuilder: K1 => String): ProtobufPartitionSaver[F, A, K1] =
    new ProtobufPartitionSaver[F, A, K1](rdd, cfg, bucketing, pathBuilder)

  def rePath(pathBuilder: K => String): ProtobufPartitionSaver[F, A, K] =
    new ProtobufPartitionSaver[F, A, K](rdd, cfg, bucketing, pathBuilder)

  override def run(
    blocker: Blocker)(implicit ss: SparkSession, F: Concurrent[F], cs: ContextShift[F]): F[Unit] =
    savePartitionedRdd(rdd, blocker, bucketing, pathBuilder)
}
