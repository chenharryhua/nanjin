package com.github.chenharryhua.nanjin.spark.saver

import cats.Parallel
import cats.effect.{Blocker, Concurrent, ContextShift}
import cats.implicits._
import cats.kernel.Eq
import com.github.chenharryhua.nanjin.spark.{fileSink, RddExt}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

sealed private[saver] trait AbstractJavaObjectSaver[F[_], A] extends AbstractSaver[F, A] {

  final override protected def writeSingleFile(
    rdd: RDD[A],
    outPath: String,
    blocker: Blocker)(implicit ss: SparkSession, F: Concurrent[F], cs: ContextShift[F]): F[Unit] =
    rdd.stream[F].through(fileSink[F](blocker).javaObject(outPath)).compile.drain

}

final class JavaObjectSaver[F[_], A](rdd: RDD[A], outPath: String, cfg: SaverConfig)
    extends AbstractJavaObjectSaver[F, A] {

  override def updateConfig(cfg: SaverConfig): JavaObjectSaver[F, A] =
    new JavaObjectSaver[F, A](rdd, outPath, cfg)

  override def errorIfExists: JavaObjectSaver[F, A]  = updateConfig(cfg.withError)
  override def overwrite: JavaObjectSaver[F, A]      = updateConfig(cfg.withOverwrite)
  override def ignoreIfExists: JavaObjectSaver[F, A] = updateConfig(cfg.withIgnore)

  def run(
    blocker: Blocker)(implicit ss: SparkSession, F: Concurrent[F], cs: ContextShift[F]): F[Unit] =
    saveRdd(rdd, outPath, cfg.evalConfig, blocker)

}

final class JavaObjectPartitionSaver[F[_], A, K: ClassTag: Eq](
  rdd: RDD[A],
  bucketing: A => Option[K],
  pathBuilder: K => String,
  val cfg: SaverConfig)
    extends AbstractJavaObjectSaver[F, A] with Partition[F, A, K] {

  override def updateConfig(cfg: SaverConfig): JavaObjectPartitionSaver[F, A, K] =
    new JavaObjectPartitionSaver[F, A, K](rdd, bucketing, pathBuilder, cfg)

  override def errorIfExists: JavaObjectPartitionSaver[F, A, K]  = updateConfig(cfg.withError)
  override def overwrite: JavaObjectPartitionSaver[F, A, K]      = updateConfig(cfg.withOverwrite)
  override def ignoreIfExists: JavaObjectPartitionSaver[F, A, K] = updateConfig(cfg.withIgnore)

  override def parallel(num: Long): JavaObjectPartitionSaver[F, A, K] =
    updateConfig(cfg.withParallel(num))

  override def reBucket[K1: ClassTag: Eq](
    bucketing: A => Option[K1],
    pathBuilder: K1 => String): JavaObjectPartitionSaver[F, A, K1] =
    new JavaObjectPartitionSaver[F, A, K1](rdd, bucketing, pathBuilder, cfg)

  override def rePath(pathBuilder: K => String): JavaObjectPartitionSaver[F, A, K] =
    new JavaObjectPartitionSaver[F, A, K](rdd, bucketing, pathBuilder, cfg)

  override def run(blocker: Blocker)(implicit
    ss: SparkSession,
    F: Concurrent[F],
    cs: ContextShift[F],
    P: Parallel[F]): F[Unit] =
    savePartitionRdd(rdd, cfg.evalConfig, blocker, bucketing, pathBuilder)
}
