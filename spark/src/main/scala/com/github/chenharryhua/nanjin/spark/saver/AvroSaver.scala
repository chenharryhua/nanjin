package com.github.chenharryhua.nanjin.spark.saver

import cats.effect.{Blocker, Concurrent, ContextShift}
import cats.implicits._
import cats.kernel.Eq
import com.github.chenharryhua.nanjin.spark.mapreduce.NJAvroKeyOutputFormat
import com.github.chenharryhua.nanjin.spark.{fileSink, utils, RddExt}
import com.sksamuel.avro4s.{Encoder, SchemaFor}
import org.apache.avro.Schema
import org.apache.avro.mapreduce.AvroJob
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.reflect.ClassTag

sealed abstract private[saver] class AbstractAvroSaver[F[_], A](
  rdd: RDD[A],
  encoder: Encoder[A],
  cfg: SaverConfig)
    extends AbstractSaver[F, A](cfg) {
  implicit private val enc: Encoder[A] = encoder

  def withEncoder(enc: Encoder[A]): AbstractAvroSaver[F, A]
  def withSchema(schema: Schema): AbstractAvroSaver[F, A]
  def overwrite: AbstractAvroSaver[F, A]
  def errorIfExists: AbstractAvroSaver[F, A]
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

final class AvroSaver[F[_], A](rdd: RDD[A], encoder: Encoder[A], cfg: SaverConfig)
    extends AbstractAvroSaver[F, A](rdd, encoder, cfg) {

  override def withEncoder(enc: Encoder[A]): AvroSaver[F, A] =
    new AvroSaver(rdd, enc, cfg)

  override def withSchema(schema: Schema): AvroSaver[F, A] = {
    val schemaFor: SchemaFor[A] = SchemaFor[A](schema)
    new AvroSaver[F, A](rdd, encoder.withSchema(schemaFor), cfg)
  }

  private def mode(sm: SaveMode): AvroSaver[F, A] =
    new AvroSaver[F, A](rdd, encoder, cfg.withSaveMode(sm))

  override def errorIfExists: AvroSaver[F, A] =
    mode(SaveMode.ErrorIfExists)

  override def overwrite: AvroSaver[F, A] =
    mode(SaveMode.Overwrite)

  override def single: AvroSaver[F, A] =
    new AvroSaver[F, A](rdd, encoder, cfg.withSingle)

  override def multi: AvroSaver[F, A] =
    new AvroSaver[F, A](rdd, encoder, cfg.withMulti)

  override def spark: AvroSaver[F, A] =
    new AvroSaver[F, A](rdd, encoder, cfg.withSpark)

  override def hadoop: AvroSaver[F, A] =
    new AvroSaver[F, A](rdd, encoder, cfg.withHadoop)

  def run(
    blocker: Blocker)(implicit ss: SparkSession, F: Concurrent[F], cs: ContextShift[F]): F[Unit] =
    saveRdd(rdd, params.outPath, blocker)

}

final class AvroPartitionSaver[F[_], A, K: ClassTag: Eq](
  rdd: RDD[A],
  encoder: Encoder[A],
  cfg: SaverConfig,
  bucketing: A => K,
  pathBuilder: K => String)
    extends AbstractAvroSaver[F, A](rdd, encoder, cfg) {

  override def withEncoder(enc: Encoder[A]): AvroPartitionSaver[F, A, K] =
    new AvroPartitionSaver(rdd, enc, cfg, bucketing, pathBuilder)

  override def withSchema(schema: Schema): AvroPartitionSaver[F, A, K] = {
    val schemaFor: SchemaFor[A] = SchemaFor[A](schema)
    new AvroPartitionSaver(rdd, encoder.withSchema(schemaFor), cfg, bucketing, pathBuilder)
  }

  private def mode(sm: SaveMode): AvroPartitionSaver[F, A, K] =
    new AvroPartitionSaver[F, A, K](rdd, encoder, cfg.withSaveMode(sm), bucketing, pathBuilder)

  override def errorIfExists: AvroPartitionSaver[F, A, K] =
    mode(SaveMode.ErrorIfExists)

  override def overwrite: AvroPartitionSaver[F, A, K] =
    mode(SaveMode.Overwrite)

  override def single: AvroPartitionSaver[F, A, K] =
    new AvroPartitionSaver[F, A, K](rdd, encoder, cfg.withSingle, bucketing, pathBuilder)

  override def multi: AvroPartitionSaver[F, A, K] =
    new AvroPartitionSaver[F, A, K](rdd, encoder, cfg.withMulti, bucketing, pathBuilder)

  override def spark: AvroPartitionSaver[F, A, K] =
    new AvroPartitionSaver[F, A, K](rdd, encoder, cfg.withSpark, bucketing, pathBuilder)

  override def hadoop: AvroPartitionSaver[F, A, K] =
    new AvroPartitionSaver[F, A, K](rdd, encoder, cfg.withHadoop, bucketing, pathBuilder)

  def run(
    blocker: Blocker)(implicit ss: SparkSession, F: Concurrent[F], cs: ContextShift[F]): F[Unit] =
    savePartitionedRdd(rdd, blocker, bucketing, pathBuilder)
}
