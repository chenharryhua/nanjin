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
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.reflect.ClassTag

sealed abstract private[saver] class AbstractParquetSaver[F[_], A](
  encoder: Encoder[A],
  constraint: TypedEncoder[A])
    extends AbstractSaver[F, A] {

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
    ss: SparkSession,
    blocker: Blocker)(implicit F: Concurrent[F], cs: ContextShift[F]): F[Unit] =
    rdd.stream[F].through(fileSink[F](blocker)(ss).parquet(outPath)).compile.drain

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

  final override protected def toDataFrame(rdd: RDD[A], ss: SparkSession): DataFrame = {
    val schema = SchemaConverters.toAvroType(SchemaConverters.toSqlType(encoder.schema).dataType)
    rdd.toDF(encoder.withSchema(SchemaFor(schema)), ss)
  }
}

final class ParquetSaver[F[_], A](
  rdd: RDD[A],
  encoder: Encoder[A],
  constraint: TypedEncoder[A],
  outPath: String,
  val cfg: SaverConfig)
    extends AbstractParquetSaver[F, A](encoder, constraint) {

  override def repartition(num: Int): ParquetSaver[F, A] =
    new ParquetSaver[F, A](rdd.repartition(num), encoder, constraint, outPath, cfg)

  override def withEncoder(enc: Encoder[A]): ParquetSaver[F, A] =
    new ParquetSaver[F, A](rdd, enc, constraint, outPath, cfg)

  override def withSchema(schema: Schema): ParquetSaver[F, A] = {
    val schemaFor: SchemaFor[A] = SchemaFor[A](schema)
    new ParquetSaver[F, A](rdd, encoder.withSchema(schemaFor), constraint, outPath, cfg)
  }

  override def updateConfig(cfg: SaverConfig): ParquetSaver[F, A] =
    new ParquetSaver[F, A](rdd, encoder, constraint, outPath, cfg)

  override def errorIfExists: ParquetSaver[F, A]  = updateConfig(cfg.withError)
  override def overwrite: ParquetSaver[F, A]      = updateConfig(cfg.withOverwrite)
  override def ignoreIfExists: ParquetSaver[F, A] = updateConfig(cfg.withIgnore)

  override def single: ParquetSaver[F, A] = updateConfig(cfg.withSingle)
  override def multi: ParquetSaver[F, A]  = updateConfig(cfg.withMulti)

  override def spark: ParquetSaver[F, A]  = updateConfig(cfg.withSpark)
  override def hadoop: ParquetSaver[F, A] = updateConfig(cfg.withHadoop)

  def run(
    blocker: Blocker)(implicit ss: SparkSession, F: Concurrent[F], cs: ContextShift[F]): F[Unit] =
    saveRdd(rdd, outPath, cfg.evalConfig, blocker)

}

final class ParquetPartitionSaver[F[_], A, K: ClassTag: Eq](
  rdd: RDD[A],
  encoder: Encoder[A],
  constraint: TypedEncoder[A],
  bucketing: A => Option[K],
  pathBuilder: K => String,
  val cfg: SaverConfig)
    extends AbstractParquetSaver[F, A](encoder, constraint) with Partition[F, A, K] {

  override def repartition(num: Int): ParquetPartitionSaver[F, A, K] =
    new ParquetPartitionSaver[F, A, K](
      rdd.repartition(num),
      encoder,
      constraint,
      bucketing,
      pathBuilder,
      cfg)

  override def withEncoder(enc: Encoder[A]): ParquetPartitionSaver[F, A, K] =
    new ParquetPartitionSaver[F, A, K](rdd, enc, constraint, bucketing, pathBuilder, cfg)

  override def withSchema(schema: Schema): ParquetPartitionSaver[F, A, K] = {
    val schemaFor = SchemaFor[A](schema)
    new ParquetPartitionSaver[F, A, K](
      rdd,
      encoder.withSchema(schemaFor),
      constraint,
      bucketing,
      pathBuilder,
      cfg)
  }

  override def updateConfig(cfg: SaverConfig): ParquetPartitionSaver[F, A, K] =
    new ParquetPartitionSaver[F, A, K](rdd, encoder, constraint, bucketing, pathBuilder, cfg)

  override def errorIfExists: ParquetPartitionSaver[F, A, K]  = updateConfig(cfg.withError)
  override def overwrite: ParquetPartitionSaver[F, A, K]      = updateConfig(cfg.withOverwrite)
  override def ignoreIfExists: ParquetPartitionSaver[F, A, K] = updateConfig(cfg.withIgnore)

  override def single: ParquetPartitionSaver[F, A, K] = updateConfig(cfg.withSingle)
  override def multi: ParquetPartitionSaver[F, A, K]  = updateConfig(cfg.withMulti)

  override def spark: ParquetPartitionSaver[F, A, K]  = updateConfig(cfg.withSpark)
  override def hadoop: ParquetPartitionSaver[F, A, K] = updateConfig(cfg.withHadoop)

  override def parallel(num: Long): ParquetPartitionSaver[F, A, K] =
    updateConfig(cfg.withParallel(num))

  override def reBucket[K1: ClassTag: Eq](
    bucketing: A => Option[K1],
    pathBuilder: K1 => String): ParquetPartitionSaver[F, A, K1] =
    new ParquetPartitionSaver[F, A, K1](rdd, encoder, constraint, bucketing, pathBuilder, cfg)

  override def rePath(pathBuilder: K => String): ParquetPartitionSaver[F, A, K] =
    new ParquetPartitionSaver[F, A, K](rdd, encoder, constraint, bucketing, pathBuilder, cfg)

  override def run(blocker: Blocker)(implicit
    ss: SparkSession,
    F: Concurrent[F],
    cs: ContextShift[F],
    P: Parallel[F]): F[Unit] =
    savePartitionRdd(rdd, cfg.evalConfig, blocker, bucketing, pathBuilder)

}
