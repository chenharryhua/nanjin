package com.github.chenharryhua.nanjin.spark.saver

import cats.Parallel
import cats.effect.{Blocker, Concurrent, ContextShift}
import cats.kernel.Eq
import com.github.chenharryhua.nanjin.spark.{fileSink, RddExt}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scalapb.GeneratedMessage

import scala.reflect.ClassTag

sealed abstract private[saver] class AbstractProtobufSaver[F[_], A](implicit
  enc: A <:< GeneratedMessage)
    extends AbstractSaver[F, A] {

  final override protected def writeSingleFile(
    rdd: RDD[A],
    outPath: String,
    blocker: Blocker)(implicit ss: SparkSession, F: Concurrent[F], cs: ContextShift[F]): F[Unit] =
    rdd.stream[F].through(fileSink[F](blocker).protobuf[A](outPath)).compile.drain

}

final class ProtobufSaver[F[_], A](rdd: RDD[A], outPath: String, cfg: SaverConfig)(implicit
  enc: A <:< GeneratedMessage)
    extends AbstractProtobufSaver[F, A] {

  def run(
    blocker: Blocker)(implicit ss: SparkSession, F: Concurrent[F], cs: ContextShift[F]): F[Unit] =
    saveRdd(rdd, outPath, cfg.evalConfig, blocker)

  override def updateConfig(cfg: SaverConfig): ProtobufSaver[F, A] =
    new ProtobufSaver[F, A](rdd, outPath, cfg)

  override def errorIfExists: ProtobufSaver[F, A]  = updateConfig(cfg.withError)
  override def overwrite: ProtobufSaver[F, A]      = updateConfig(cfg.withOverwrite)
  override def ignoreIfExists: ProtobufSaver[F, A] = updateConfig(cfg.withIgnore)

}

final class ProtobufPartitionSaver[F[_], A, K: ClassTag: Eq](
  rdd: RDD[A],
  bucketing: A => K,
  pathBuilder: K => String,
  val cfg: SaverConfig)(implicit enc: A <:< GeneratedMessage)
    extends AbstractProtobufSaver[F, A] with Partition[F, A, K] {

  override def updateConfig(cfg: SaverConfig): ProtobufPartitionSaver[F, A, K] =
    new ProtobufPartitionSaver[F, A, K](rdd, bucketing, pathBuilder, cfg)

  override def errorIfExists: ProtobufPartitionSaver[F, A, K]  = updateConfig(cfg.withError)
  override def overwrite: ProtobufPartitionSaver[F, A, K]      = updateConfig(cfg.withOverwrite)
  override def ignoreIfExists: ProtobufPartitionSaver[F, A, K] = updateConfig(cfg.withIgnore)

  override def parallel(num: Long): ProtobufPartitionSaver[F, A, K] =
    updateConfig(cfg.withParallel(num))

  override def reBucket[K1: ClassTag: Eq](
    bucketing: A => K1,
    pathBuilder: K1 => String): ProtobufPartitionSaver[F, A, K1] =
    new ProtobufPartitionSaver[F, A, K1](rdd, bucketing, pathBuilder, cfg)

  override def rePath(pathBuilder: K => String): ProtobufPartitionSaver[F, A, K] =
    new ProtobufPartitionSaver[F, A, K](rdd, bucketing, pathBuilder, cfg)

  override def run(blocker: Blocker)(implicit
    ss: SparkSession,
    F: Concurrent[F],
    cs: ContextShift[F],
    P: Parallel[F]): F[Unit] =
    savePartitionRdd(rdd, cfg.evalConfig, blocker, bucketing, pathBuilder)
}
