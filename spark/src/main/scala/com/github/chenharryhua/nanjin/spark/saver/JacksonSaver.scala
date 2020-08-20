package com.github.chenharryhua.nanjin.spark.saver

import cats.Parallel
import cats.effect.{Blocker, Concurrent, ContextShift}
import cats.implicits._
import cats.kernel.Eq
import com.github.chenharryhua.nanjin.spark.mapreduce.NJJacksonKeyOutputFormat
import com.github.chenharryhua.nanjin.spark.{fileSink, utils, RddExt}
import com.sksamuel.avro4s.Encoder
import org.apache.avro.mapreduce.AvroJob
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.reflect.ClassTag

sealed abstract private[saver] class AbstractJacksonSaver[F[_], A](encoder: Encoder[A])
    extends AbstractSaver[F, A] {
  implicit private val enc: Encoder[A] = encoder

  def single: AbstractJacksonSaver[F, A]
  def multi: AbstractJacksonSaver[F, A]

  final override protected def writeSingleFile(
    rdd: RDD[A],
    outPath: String,
    blocker: Blocker)(implicit ss: SparkSession, F: Concurrent[F], cs: ContextShift[F]): F[Unit] =
    rdd.stream[F].through(fileSink[F](blocker).jackson(outPath)).compile.drain

  final override protected def writeMultiFiles(
    rdd: RDD[A],
    outPath: String,
    ss: SparkSession): Unit = {
    val job = Job.getInstance(ss.sparkContext.hadoopConfiguration)
    AvroJob.setOutputKeySchema(job, enc.schema)
    ss.sparkContext.hadoopConfiguration.addResource(job.getConfiguration)
    utils.genericRecordPair(rdd, encoder).saveAsNewAPIHadoopFile[NJJacksonKeyOutputFormat](outPath)
  }

  final override protected def toDataFrame(rdd: RDD[A])(implicit ss: SparkSession): DataFrame =
    rdd.toDF

}

final class JacksonSaver[F[_], A](
  rdd: RDD[A],
  encoder: Encoder[A],
  outPath: String,
  cfg: SaverConfig)
    extends AbstractJacksonSaver[F, A](encoder) {

  override def updateConfig(cfg: SaverConfig): JacksonSaver[F, A] =
    new JacksonSaver(rdd, encoder, outPath, cfg)

  override def errorIfExists: JacksonSaver[F, A]  = updateConfig(cfg.withError)
  override def overwrite: JacksonSaver[F, A]      = updateConfig(cfg.withOverwrite)
  override def ignoreIfExists: JacksonSaver[F, A] = updateConfig(cfg.withIgnore)

  override def single: JacksonSaver[F, A] = updateConfig(cfg.withSingle)
  override def multi: JacksonSaver[F, A]  = updateConfig(cfg.withMulti)

  def run(
    blocker: Blocker)(implicit ss: SparkSession, F: Concurrent[F], cs: ContextShift[F]): F[Unit] =
    saveRdd(rdd, outPath, cfg.evalConfig, blocker)

}

final class JacksonPartitionSaver[F[_], A, K: ClassTag: Eq](
  rdd: RDD[A],
  encoder: Encoder[A],
  bucketing: A => Option[K],
  pathBuilder: K => String,
  val cfg: SaverConfig)
    extends AbstractJacksonSaver[F, A](encoder) with Partition[F, A, K] {

  override def updateConfig(cfg: SaverConfig): JacksonPartitionSaver[F, A, K] =
    new JacksonPartitionSaver[F, A, K](rdd, encoder, bucketing, pathBuilder, cfg)

  override def errorIfExists: JacksonPartitionSaver[F, A, K]  = updateConfig(cfg.withError)
  override def overwrite: JacksonPartitionSaver[F, A, K]      = updateConfig(cfg.withOverwrite)
  override def ignoreIfExists: JacksonPartitionSaver[F, A, K] = updateConfig(cfg.withIgnore)

  override def single: JacksonPartitionSaver[F, A, K] = updateConfig(cfg.withSingle)
  override def multi: JacksonPartitionSaver[F, A, K]  = updateConfig(cfg.withMulti)

  override def parallel(num: Long): JacksonPartitionSaver[F, A, K] =
    updateConfig(cfg.withParallel(num))

  override def reBucket[K1: ClassTag: Eq](
    bucketing: A => Option[K1],
    pathBuilder: K1 => String): JacksonPartitionSaver[F, A, K1] =
    new JacksonPartitionSaver[F, A, K1](rdd, encoder, bucketing, pathBuilder, cfg)

  override def rePath(pathBuilder: K => String): JacksonPartitionSaver[F, A, K] =
    new JacksonPartitionSaver[F, A, K](rdd, encoder, bucketing, pathBuilder, cfg)

  override def run(blocker: Blocker)(implicit
    ss: SparkSession,
    F: Concurrent[F],
    cs: ContextShift[F],
    P: Parallel[F]): F[Unit] =
    savePartitionRdd(rdd, cfg.evalConfig, blocker, bucketing, pathBuilder)
}
