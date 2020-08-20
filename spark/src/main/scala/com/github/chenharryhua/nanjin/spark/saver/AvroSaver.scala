package com.github.chenharryhua.nanjin.spark.saver

import cats.Parallel
import cats.effect.{Blocker, Concurrent, ContextShift}
import cats.implicits._
import cats.kernel.Eq
import com.github.chenharryhua.nanjin.spark.mapreduce.NJAvroKeyOutputFormat
import com.github.chenharryhua.nanjin.spark.{fileSink, utils, RddExt}
import com.sksamuel.avro4s.{Encoder, SchemaFor}
import monocle.Lens
import org.apache.avro.Schema
import org.apache.avro.mapreduce.AvroJob
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.reflect.ClassTag

sealed abstract private[saver] class AbstractAvroSaver[F[_], A](encoder: Encoder[A])
    extends AbstractSaver[F, A] {
  implicit private val enc: Encoder[A] = encoder

  def withEncoder(enc: Encoder[A]): AbstractAvroSaver[F, A]
  def withSchema(schema: Schema): AbstractAvroSaver[F, A]

  def single: AbstractAvroSaver[F, A]
  def multi: AbstractAvroSaver[F, A]
  def spark: AbstractAvroSaver[F, A]
  def hadoop: AbstractAvroSaver[F, A]

  final override protected def writeSingleFile(
    rdd: RDD[A],
    outPath: String,
    blocker: Blocker)(implicit ss: SparkSession, F: Concurrent[F], cs: ContextShift[F]): F[Unit] =
    rdd.stream[F].through(fileSink[F](blocker).avro(outPath)).compile.drain

  final override protected def writeMultiFiles(
    rdd: RDD[A],
    outPath: String,
    ss: SparkSession): Unit = {
    val job = Job.getInstance(ss.sparkContext.hadoopConfiguration)
    AvroJob.setOutputKeySchema(job, enc.schema)
    ss.sparkContext.hadoopConfiguration.addResource(job.getConfiguration)
    utils.genericRecordPair(rdd, encoder).saveAsNewAPIHadoopFile[NJAvroKeyOutputFormat](outPath)
  }

  final override protected def toDataFrame(rdd: RDD[A])(implicit ss: SparkSession): DataFrame =
    rdd.toDF

}

final class AvroSaver[F[_], A](
  rdd: RDD[A],
  encoder: Encoder[A],
  outPath: String,
  val cfg: SaverConfig)
    extends AbstractAvroSaver[F, A](encoder) {

  override def withEncoder(enc: Encoder[A]): AvroSaver[F, A] =
    new AvroSaver(rdd, enc, outPath, cfg)

  override def withSchema(schema: Schema): AvroSaver[F, A] = {
    val schemaFor: SchemaFor[A] = SchemaFor[A](schema)
    new AvroSaver[F, A](rdd, encoder.withSchema(schemaFor), outPath, cfg)
  }

  override def updateConfig(cfg: SaverConfig): AvroSaver[F, A] =
    new AvroSaver[F, A](rdd, encoder, outPath, cfg)

  override def errorIfExists: AvroSaver[F, A]  = updateConfig(cfg.withError)
  override def overwrite: AvroSaver[F, A]      = updateConfig(cfg.withOverwrite)
  override def ignoreIfExists: AvroSaver[F, A] = updateConfig(cfg.withIgnore)

  override def single: AvroSaver[F, A] = updateConfig(cfg.withSingle)
  override def multi: AvroSaver[F, A]  = updateConfig(cfg.withMulti)

  override def spark: AvroSaver[F, A]  = updateConfig(cfg.withSpark)
  override def hadoop: AvroSaver[F, A] = updateConfig(cfg.withHadoop)

  def run(
    blocker: Blocker)(implicit ss: SparkSession, F: Concurrent[F], cs: ContextShift[F]): F[Unit] =
    saveRdd(rdd, outPath, cfg.evalConfig, blocker)

}

final class AvroPartitionSaver[F[_], A, K: ClassTag: Eq](
  rdd: RDD[A],
  encoder: Encoder[A],
  bucketing: A => Option[K],
  pathBuilder: K => String,
  val cfg: SaverConfig)
    extends AbstractAvroSaver[F, A](encoder) with Partition[F, A, K] {

  override def updateConfig(cfg: SaverConfig): AvroPartitionSaver[F, A, K] =
    new AvroPartitionSaver[F, A, K](rdd, encoder, bucketing, pathBuilder, cfg)

  override def withEncoder(enc: Encoder[A]): AvroPartitionSaver[F, A, K] =
    new AvroPartitionSaver(rdd, enc, bucketing, pathBuilder, cfg)

  override def withSchema(schema: Schema): AvroPartitionSaver[F, A, K] = {
    val schemaFor: SchemaFor[A] = SchemaFor[A](schema)
    new AvroPartitionSaver(rdd, encoder.withSchema(schemaFor), bucketing, pathBuilder, cfg)
  }

  override def errorIfExists: AvroPartitionSaver[F, A, K]  = updateConfig(cfg.withError)
  override def overwrite: AvroPartitionSaver[F, A, K]      = updateConfig(cfg.withOverwrite)
  override def ignoreIfExists: AvroPartitionSaver[F, A, K] = updateConfig(cfg.withIgnore)

  override def single: AvroPartitionSaver[F, A, K] = updateConfig(cfg.withSingle)
  override def multi: AvroPartitionSaver[F, A, K]  = updateConfig(cfg.withMulti)

  override def spark: AvroPartitionSaver[F, A, K]  = updateConfig(cfg.withSpark)
  override def hadoop: AvroPartitionSaver[F, A, K] = updateConfig(cfg.withHadoop)

  override def parallel(num: Long): AvroPartitionSaver[F, A, K] =
    updateConfig(cfg.withParallel(num))

  override def reBucket[K1: ClassTag: Eq](
    bucketing: A => Option[K1],
    pathBuilder: K1 => String): AvroPartitionSaver[F, A, K1] =
    new AvroPartitionSaver[F, A, K1](rdd, encoder, bucketing, pathBuilder, cfg)

  override def rePath(pathBuilder: K => String): AvroPartitionSaver[F, A, K] =
    new AvroPartitionSaver[F, A, K](rdd, encoder, bucketing, pathBuilder, cfg)

  override def run(blocker: Blocker)(implicit
    ss: SparkSession,
    F: Concurrent[F],
    CS: ContextShift[F],
    P: Parallel[F]): F[Unit] =
    savePartitionRdd(rdd, cfg.evalConfig, blocker, bucketing, pathBuilder)
}
