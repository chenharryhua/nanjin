package com.github.chenharryhua.nanjin.spark.saver

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

final class ParquetSaver[F[_], A](
  rdd: RDD[A],
  encoder: Encoder[A],
  constraint: TypedEncoder[A],
  cfg: SaverConfig)
    extends AbstractSaver[F, A](cfg) {

  implicit private val te: TypedEncoder[A] = constraint
  implicit private val enc: Encoder[A]     = encoder

  def withEncoder(enc: Encoder[A]): ParquetSaver[F, A] =
    new ParquetSaver[F, A](rdd, enc, constraint, cfg)

  def withSchema(schema: Schema): ParquetSaver[F, A] = {
    val schemaFor: SchemaFor[A] = SchemaFor[A](schema)
    new ParquetSaver[F, A](rdd, encoder.withSchema(schemaFor), constraint, cfg)
  }

  private def mode(sm: SaveMode): ParquetSaver[F, A] =
    new ParquetSaver[F, A](rdd, encoder, constraint, cfg.withSaveMode(sm))

  def overwrite: ParquetSaver[F, A]     = mode(SaveMode.Overwrite)
  def errorIfExists: ParquetSaver[F, A] = mode(SaveMode.ErrorIfExists)

  def single: ParquetSaver[F, A] =
    new ParquetSaver[F, A](rdd, encoder, constraint, cfg.withSingle)

  def multi: ParquetSaver[F, A] =
    new ParquetSaver[F, A](rdd, encoder, constraint, cfg.withMulti)

  def spark: ParquetSaver[F, A] =
    new ParquetSaver[F, A](rdd, encoder, constraint, cfg.withSpark)

  def hadoop: ParquetSaver[F, A] =
    new ParquetSaver[F, A](rdd, encoder, constraint, cfg.withHadoop)

  override protected def writeSingleFile(rdd: RDD[A], outPath: String, blocker: Blocker)(implicit
    ss: SparkSession,
    F: Concurrent[F],
    cs: ContextShift[F]): F[Unit] =
    rdd.stream[F].through(fileSink[F](blocker).parquet(outPath)).compile.drain

  override protected def writeMultiFiles(rdd: RDD[A], outPath: String, ss: SparkSession): Unit = {
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
