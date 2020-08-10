package com.github.chenharryhua.nanjin.spark.saver

import cats.effect.{Blocker, Concurrent, ContextShift}
import cats.implicits._
import cats.kernel.Eq
import com.github.chenharryhua.nanjin.spark.{fileSink, RddExt}
import io.circe.Encoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.reflect.ClassTag

final class CirceJsonSaver[F[_], A](rdd: RDD[A], encoder: Encoder[A], cfg: SaverConfig)
    extends AbstractSaver[F, A](cfg) {

  implicit private val enc: Encoder[A] = encoder

  private def mode(sm: SaveMode): CirceJsonSaver[F, A] =
    new CirceJsonSaver[F, A](rdd, encoder, cfg.withSaveMode(sm))

  def overwrite: CirceJsonSaver[F, A]     = mode(SaveMode.Overwrite)
  def errorIfExists: CirceJsonSaver[F, A] = mode(SaveMode.ErrorIfExists)

  def single: CirceJsonSaver[F, A] =
    new CirceJsonSaver[F, A](rdd, encoder, cfg.withSingle)

  def multi: CirceJsonSaver[F, A] =
    new CirceJsonSaver[F, A](rdd, encoder, cfg.withMulti)

  override protected def writeSingleFile(rdd: RDD[A], outPath: String, blocker: Blocker)(implicit
    ss: SparkSession,
    F: Concurrent[F],
    cs: ContextShift[F]): F[Unit] =
    rdd.stream[F].through(fileSink[F](blocker).circe(outPath)).compile.drain

  override protected def writeMultiFiles(rdd: RDD[A], outPath: String, ss: SparkSession): Unit =
    rdd.map(encoder(_).noSpaces).saveAsTextFile(outPath)

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
