package com.github.chenharryhua.nanjin.spark.saver

import cats.effect.{Blocker, Concurrent, ContextShift}
import cats.implicits._
import cats.kernel.Eq
import com.github.chenharryhua.nanjin.spark.{fileSink, RddExt}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.reflect.ClassTag

sealed abstract private[saver] class AbstractJavaObjectSaver[F[_], A](rdd: RDD[A], cfg: SaverConfig)
    extends AbstractSaver[F, A](cfg) {
  def overwrite: AbstractJavaObjectSaver[F, A]
  def errorIfExists: AbstractJavaObjectSaver[F, A]

  final override protected def writeSingleFile(
    rdd: RDD[A],
    outPath: String,
    blocker: Blocker)(implicit ss: SparkSession, F: Concurrent[F], cs: ContextShift[F]): F[Unit] =
    rdd.stream[F].through(fileSink[F](blocker).javaObject(outPath)).compile.drain

}

final class JavaObjectSaver[F[_], A](rdd: RDD[A], cfg: SaverConfig)
    extends AbstractJavaObjectSaver[F, A](rdd, cfg) {

  private def mode(sm: SaveMode): JavaObjectSaver[F, A] =
    new JavaObjectSaver[F, A](rdd, cfg.withSaveMode(sm))

  override def overwrite: JavaObjectSaver[F, A]     = mode(SaveMode.Overwrite)
  override def errorIfExists: JavaObjectSaver[F, A] = mode(SaveMode.ErrorIfExists)

  override def run(
    blocker: Blocker)(implicit ss: SparkSession, F: Concurrent[F], cs: ContextShift[F]): F[Unit] =
    saveRdd(rdd, params.outPath, blocker)

}

final class JavaObjectPartitionSaver[F[_], A, K: ClassTag: Eq](
  rdd: RDD[A],
  cfg: SaverConfig,
  bucketing: A => K,
  pathBuilder: K => String)
    extends AbstractJavaObjectSaver[F, A](rdd, cfg) {

  override def overwrite: AbstractJavaObjectSaver[F, A] =
    new JavaObjectPartitionSaver(rdd, cfg.withSaveMode(SaveMode.Overwrite), bucketing, pathBuilder)

  override def errorIfExists: AbstractJavaObjectSaver[F, A] =
    new JavaObjectPartitionSaver(
      rdd,
      cfg.withSaveMode(SaveMode.ErrorIfExists),
      bucketing,
      pathBuilder)

  override def run(
    blocker: Blocker)(implicit ss: SparkSession, F: Concurrent[F], cs: ContextShift[F]): F[Unit] =
    savePartitionedRdd(rdd, blocker, bucketing, pathBuilder)
}
