package com.github.chenharryhua.nanjin.spark.saver

import cats.Parallel
import cats.effect.{Blocker, Concurrent, ContextShift}
import cats.implicits._
import cats.kernel.Eq
import com.github.chenharryhua.nanjin.spark.{fileSink, RddExt}
import com.sksamuel.avro4s.{Encoder, SchemaFor}
import frameless.TypedEncoder
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.mapreduce.Job
import org.apache.parquet.avro.{AvroParquetOutputFormat, GenericDataSupplier}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.reflect.ClassTag

sealed abstract private[saver] class AbstractParquetSaver[F[_], A](
  rdd: RDD[A],
  encoder: Encoder[A],
  constraint: TypedEncoder[A],
  cfg: SaverConfig)
    extends AbstractSaver[F, A](cfg) {

  implicit private val te: TypedEncoder[A] = constraint
  implicit private val enc: Encoder[A]     = encoder

  def withEncoder(enc: Encoder[A]): AbstractParquetSaver[F, A]
  def withSchema(schema: Schema): AbstractParquetSaver[F, A]
  def single: AbstractParquetSaver[F, A]
  def multi: AbstractParquetSaver[F, A]
  def spark: AbstractParquetSaver[F, A]
  def hadoop: AbstractParquetSaver[F, A]

  final override protected def writeSingleFile(
    rdd: RDD[A],
    outPath: String,
    blocker: Blocker)(implicit ss: SparkSession, F: Concurrent[F], cs: ContextShift[F]): F[Unit] =
    rdd.stream[F].through(fileSink[F](blocker).parquet(outPath)).compile.drain

  final override protected def writeMultiFiles(
    rdd: RDD[A],
    outPath: String,
    ss: SparkSession): Unit = {
    val job = Job.getInstance(ss.sparkContext.hadoopConfiguration)
    AvroParquetOutputFormat.setAvroDataSupplier(job, classOf[GenericDataSupplier])
    AvroParquetOutputFormat.setSchema(job, encoder.schema)
    ss.sparkContext.hadoopConfiguration.addResource(job.getConfiguration)
    rdd // null as java Void
      .map(a => (null, encoder.encode(a).asInstanceOf[GenericRecord]))
      .saveAsNewAPIHadoopFile(
        outPath,
        classOf[Void],
        classOf[GenericRecord],
        classOf[AvroParquetOutputFormat[GenericRecord]])
  }

  final override protected def toDataFrame(rdd: RDD[A])(implicit ss: SparkSession): DataFrame =
    rdd.toDF

}

final class ParquetSaver[F[_], A](
  rdd: RDD[A],
  encoder: Encoder[A],
  constraint: TypedEncoder[A],
  cfg: SaverConfig)
    extends AbstractParquetSaver[F, A](rdd, encoder, constraint, cfg) {

  override def withEncoder(enc: Encoder[A]): ParquetSaver[F, A] =
    new ParquetSaver[F, A](rdd, enc, constraint, cfg)

  override def withSchema(schema: Schema): ParquetSaver[F, A] = {
    val schemaFor: SchemaFor[A] = SchemaFor[A](schema)
    new ParquetSaver[F, A](rdd, encoder.withSchema(schemaFor), constraint, cfg)
  }

  private def mode(sm: SaveMode): ParquetSaver[F, A] =
    new ParquetSaver[F, A](rdd, encoder, constraint, cfg.withSaveMode(sm))

  override def overwrite: ParquetSaver[F, A]      = mode(SaveMode.Overwrite)
  override def errorIfExists: ParquetSaver[F, A]  = mode(SaveMode.ErrorIfExists)
  override def ignoreIfExists: ParquetSaver[F, A] = mode(SaveMode.Ignore)

  override def single: ParquetSaver[F, A] =
    new ParquetSaver[F, A](rdd, encoder, constraint, cfg.withSingle)

  override def multi: ParquetSaver[F, A] =
    new ParquetSaver[F, A](rdd, encoder, constraint, cfg.withMulti)

  override def spark: ParquetSaver[F, A] =
    new ParquetSaver[F, A](rdd, encoder, constraint, cfg.withSpark)

  override def hadoop: ParquetSaver[F, A] =
    new ParquetSaver[F, A](rdd, encoder, constraint, cfg.withHadoop)

  def run(
    blocker: Blocker)(implicit ss: SparkSession, F: Concurrent[F], cs: ContextShift[F]): F[Unit] =
    saveRdd(rdd, params.outPath, blocker)

}

final class ParquetPartitionSaver[F[_], A, K: ClassTag: Eq](
  rdd: RDD[A],
  encoder: Encoder[A],
  constraint: TypedEncoder[A],
  cfg: SaverConfig,
  bucketing: A => K,
  pathBuilder: K => String)
    extends AbstractParquetSaver[F, A](rdd, encoder, constraint, cfg) with Partition[F, A, K] {

  override def withEncoder(enc: Encoder[A]): ParquetPartitionSaver[F, A, K] =
    new ParquetPartitionSaver[F, A, K](rdd, enc, constraint, cfg, bucketing, pathBuilder)

  override def withSchema(schema: Schema): ParquetPartitionSaver[F, A, K] = {
    val schemaFor = SchemaFor[A](schema)
    new ParquetPartitionSaver[F, A, K](
      rdd,
      encoder.withSchema(schemaFor),
      constraint,
      cfg,
      bucketing,
      pathBuilder)
  }

  override def overwrite: ParquetPartitionSaver[F, A, K] =
    new ParquetPartitionSaver[F, A, K](
      rdd,
      encoder,
      constraint,
      cfg.withSaveMode(SaveMode.Overwrite),
      bucketing,
      pathBuilder)

  override def errorIfExists: ParquetPartitionSaver[F, A, K] =
    new ParquetPartitionSaver[F, A, K](
      rdd,
      encoder,
      constraint,
      cfg.withSaveMode(SaveMode.ErrorIfExists),
      bucketing,
      pathBuilder)

  override def ignoreIfExists: ParquetPartitionSaver[F, A, K] =
    new ParquetPartitionSaver[F, A, K](
      rdd,
      encoder,
      constraint,
      cfg.withSaveMode(SaveMode.Ignore),
      bucketing,
      pathBuilder)

  override def single: ParquetPartitionSaver[F, A, K] =
    new ParquetPartitionSaver[F, A, K](
      rdd,
      encoder,
      constraint,
      cfg.withSingle,
      bucketing,
      pathBuilder)

  override def multi: ParquetPartitionSaver[F, A, K] =
    new ParquetPartitionSaver[F, A, K](
      rdd,
      encoder,
      constraint,
      cfg.withMulti,
      bucketing,
      pathBuilder)

  override def spark: ParquetPartitionSaver[F, A, K] =
    new ParquetPartitionSaver[F, A, K](
      rdd,
      encoder,
      constraint,
      cfg.withSpark,
      bucketing,
      pathBuilder)

  override def hadoop: ParquetPartitionSaver[F, A, K] =
    new ParquetPartitionSaver[F, A, K](
      rdd,
      encoder,
      constraint,
      cfg.withHadoop,
      bucketing,
      pathBuilder)

  override def reBucket[K1: ClassTag: Eq](
    bucketing: A => K1,
    pathBuilder: K1 => String): ParquetPartitionSaver[F, A, K1] =
    new ParquetPartitionSaver[F, A, K1](rdd, encoder, constraint, cfg, bucketing, pathBuilder)

  override def rePath(pathBuilder: K => String): ParquetPartitionSaver[F, A, K] =
    new ParquetPartitionSaver[F, A, K](rdd, encoder, constraint, cfg, bucketing, pathBuilder)

  override def parallel(num: Long): ParquetPartitionSaver[F, A, K] =
    new ParquetPartitionSaver[F, A, K](
      rdd,
      encoder,
      constraint,
      cfg.withParallism(num),
      bucketing,
      pathBuilder)

  override def run(blocker: Blocker)(implicit
    ss: SparkSession,
    F: Concurrent[F],
    cs: ContextShift[F],
    P: Parallel[F]): F[Unit] =
    savePartitionedRdd(rdd, blocker, bucketing, pathBuilder)
}
