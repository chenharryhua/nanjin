package com.github.chenharryhua.nanjin.spark.saver

import cats.effect.{Blocker, Concurrent, ContextShift}
import cats.implicits._
import cats.kernel.Eq
import com.github.chenharryhua.nanjin.spark.{fileSink, RddExt}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.reflect.ClassTag

final class JavaObjectSaver[F[_], A](rdd: RDD[A], cfg: SaverConfig)
    extends AbstractSaver[F, A](cfg) {

  private def mode(sm: SaveMode): JavaObjectSaver[F, A] =
    new JavaObjectSaver[F, A](rdd, cfg.withSaveMode(sm))

  def overwrite: JavaObjectSaver[F, A]     = mode(SaveMode.Overwrite)
  def errorIfExists: JavaObjectSaver[F, A] = mode(SaveMode.ErrorIfExists)

  override protected def writeSingleFile(rdd: RDD[A], outPath: String, blocker: Blocker)(implicit
    ss: SparkSession,
    F: Concurrent[F],
    cs: ContextShift[F]): F[Unit] =
    rdd.stream[F].through(fileSink[F](blocker).javaObject(outPath)).compile.drain

  def run(
    blocker: Blocker)(implicit ss: SparkSession, F: Concurrent[F], cs: ContextShift[F]): F[Unit] =
    saveRdd(rdd, params.outPath, blocker)

  def runPartition[K: ClassTag: Eq](blocker: Blocker)(bucketing: A => K)(
    pathBuilder: K => String)(implicit
    F: Concurrent[F],
    ce: ContextShift[F],
    ss: SparkSession): F[Unit] =
    savePartitionedRdd(rdd, blocker, bucketing, pathBuilder)
}
