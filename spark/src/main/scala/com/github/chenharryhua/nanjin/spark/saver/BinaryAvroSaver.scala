package com.github.chenharryhua.nanjin.spark.saver

import cats.effect.{Blocker, Concurrent, ContextShift}
import cats.implicits._
import cats.kernel.Eq
import com.github.chenharryhua.nanjin.spark.{fileSink, RddExt}
import com.sksamuel.avro4s.Encoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.reflect.ClassTag

final class BinaryAvroSaver[F[_], A](rdd: RDD[A], encoder: Encoder[A], cfg: SaverConfig)
    extends AbstractSaver[F, A](cfg) {
  implicit private val enc: Encoder[A] = encoder

  private def mode(sm: SaveMode): BinaryAvroSaver[F, A] =
    new BinaryAvroSaver(rdd, encoder, cfg.withSaveMode(sm))

  def overwrite: BinaryAvroSaver[F, A]     = mode(SaveMode.Overwrite)
  def errorIfExists: BinaryAvroSaver[F, A] = mode(SaveMode.ErrorIfExists)

  override protected def writeSingleFile(rdd: RDD[A], outPath: String, blocker: Blocker)(implicit
    ss: SparkSession,
    F: Concurrent[F],
    cs: ContextShift[F]): F[Unit] =
    rdd.stream[F].through(fileSink[F](blocker).binAvro(outPath)).compile.drain

  override protected def toDataFrame(rdd: RDD[A])(implicit ss: SparkSession): DataFrame =
    rdd.toDF

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
