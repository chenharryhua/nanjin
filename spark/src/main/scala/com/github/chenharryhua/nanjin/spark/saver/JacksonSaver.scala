package com.github.chenharryhua.nanjin.spark.saver

import cats.effect.{Blocker, Concurrent, ContextShift}
import cats.implicits._
import cats.kernel.Eq
import com.github.chenharryhua.nanjin.spark.mapreduce.NJJacksonKeyOutputFormat
import com.github.chenharryhua.nanjin.spark.{fileSink, utils, RddExt}
import com.sksamuel.avro4s.Encoder
import org.apache.avro.mapreduce.AvroJob
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.reflect.ClassTag

sealed abstract private[saver] class AbstractJacksonSaver[F[_], A](
  rdd: RDD[A],
  encoder: Encoder[A],
  cfg: SaverConfig)
    extends AbstractSaver[F, A](cfg) {
  implicit private val enc: Encoder[A] = encoder

  def overwrite: AbstractJacksonSaver[F, A]
  def errorIfExists: AbstractJacksonSaver[F, A]
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

final class JacksonSaver[F[_], A](rdd: RDD[A], encoder: Encoder[A], cfg: SaverConfig)
    extends AbstractJacksonSaver[F, A](rdd, encoder, cfg) {

  private def mode(sm: SaveMode): JacksonSaver[F, A] =
    new JacksonSaver(rdd, encoder, cfg.withSaveMode(sm))

  override def overwrite: JacksonSaver[F, A]     = mode(SaveMode.Overwrite)
  override def errorIfExists: JacksonSaver[F, A] = mode(SaveMode.ErrorIfExists)

  override def single: JacksonSaver[F, A] =
    new JacksonSaver[F, A](rdd, encoder, cfg.withSingle)

  override def multi: JacksonSaver[F, A] =
    new JacksonSaver[F, A](rdd, encoder, cfg.withMulti)

  override def run(
    blocker: Blocker)(implicit ss: SparkSession, F: Concurrent[F], cs: ContextShift[F]): F[Unit] =
    saveRdd(rdd, params.outPath, blocker)

}

final class JacksonPartitionSaver[F[_], A, K: ClassTag: Eq](
  rdd: RDD[A],
  encoder: Encoder[A],
  cfg: SaverConfig,
  bucketing: A => K,
  pathBuilder: K => String)
    extends AbstractJacksonSaver[F, A](rdd, encoder, cfg) {

  override def overwrite: JacksonPartitionSaver[F, A, K] =
    new JacksonPartitionSaver(
      rdd,
      encoder,
      cfg.withSaveMode(SaveMode.Overwrite),
      bucketing,
      pathBuilder)

  override def errorIfExists: JacksonPartitionSaver[F, A, K] =
    new JacksonPartitionSaver(
      rdd,
      encoder,
      cfg.withSaveMode(SaveMode.ErrorIfExists),
      bucketing,
      pathBuilder)

  override def single: JacksonPartitionSaver[F, A, K] =
    new JacksonPartitionSaver(rdd, encoder, cfg.withSingle, bucketing, pathBuilder)

  override def multi: JacksonPartitionSaver[F, A, K] =
    new JacksonPartitionSaver(rdd, encoder, cfg.withMulti, bucketing, pathBuilder)

  def reBucket[K1: ClassTag: Eq](
    bucketing: A => K1,
    pathBuilder: K1 => String): JacksonPartitionSaver[F, A, K1] =
    new JacksonPartitionSaver[F, A, K1](rdd, encoder, cfg, bucketing, pathBuilder)

  def rePath(pathBuilder: K => String): JacksonPartitionSaver[F, A, K] =
    new JacksonPartitionSaver[F, A, K](rdd, encoder, cfg, bucketing, pathBuilder)

  override def run(
    blocker: Blocker)(implicit ss: SparkSession, F: Concurrent[F], cs: ContextShift[F]): F[Unit] =
    savePartitionedRdd(rdd, blocker, bucketing, pathBuilder)
}
