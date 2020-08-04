package com.github.chenharryhua.nanjin.spark.saver

import cats.effect.{Blocker, ContextShift, Sync}
import cats.implicits._
import com.github.chenharryhua.nanjin.spark.{fileSink, RddExt}
import com.sksamuel.avro4s.{Encoder, SchemaFor}
import frameless.{TypedDataset, TypedEncoder}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.mapreduce.Job
import org.apache.parquet.avro.{AvroParquetOutputFormat, GenericDataSupplier}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

final class ParquetSaver[F[_], A](
  rdd: RDD[A],
  encoder: Encoder[A],
  outPath: String,
  constraint: TypedEncoder[A],
  cfg: SaverConfig) {

  implicit private val te: TypedEncoder[A] = constraint
  implicit private val enc: Encoder[A]     = encoder

  val params: SaverParams = cfg.evalConfig

  def withEncoder(enc: Encoder[A]): ParquetSaver[F, A] =
    new ParquetSaver[F, A](rdd, enc, outPath, constraint, cfg)

  def withSchema(schema: Schema): ParquetSaver[F, A] = {
    val schemaFor: SchemaFor[A] = SchemaFor[A](schema)
    new ParquetSaver[F, A](rdd, encoder.withSchema(schemaFor), outPath, constraint, cfg)
  }

  def mode(sm: SaveMode): ParquetSaver[F, A] =
    new ParquetSaver[F, A](rdd, encoder, outPath, constraint, cfg.withSaveMode(sm))

  def overwrite: ParquetSaver[F, A]     = mode(SaveMode.Overwrite)
  def errorIfExists: ParquetSaver[F, A] = mode(SaveMode.ErrorIfExists)

  def single: ParquetSaver[F, A] =
    new ParquetSaver[F, A](rdd, encoder, outPath, constraint, cfg.withSingle)

  def multi: ParquetSaver[F, A] =
    new ParquetSaver[F, A](rdd, encoder, outPath, constraint, cfg.withMulti)

  def spark: ParquetSaver[F, A] =
    new ParquetSaver[F, A](rdd, encoder, outPath, constraint, cfg.withSpark)

  def hadoop: ParquetSaver[F, A] =
    new ParquetSaver[F, A](rdd, encoder, outPath, constraint, cfg.withHadoop)

  private def writeSingleFile(
    blocker: Blocker)(implicit ss: SparkSession, F: Sync[F], cs: ContextShift[F]): F[Unit] =
    rdd.stream[F].through(fileSink[F](blocker).parquet(outPath)).compile.drain

  private def writeMultiFiles(ss: SparkSession): Unit = {
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

  def run(blocker: Blocker)(implicit ss: SparkSession, F: Sync[F], cs: ContextShift[F]): F[Unit] =
    params.singleOrMulti match {
      case SingleOrMulti.Single =>
        params.saveMode match {
          case SaveMode.Append => F.raiseError(new Exception("append mode is not support"))
          case SaveMode.Overwrite =>
            fileSink[F](blocker).delete(outPath) >> writeSingleFile(blocker)

          case SaveMode.ErrorIfExists =>
            fileSink[F](blocker).isExist(outPath).flatMap {
              case true  => F.raiseError(new Exception(s"$outPath already exist"))
              case false => writeSingleFile(blocker)
            }
          case SaveMode.Ignore =>
            fileSink[F](blocker).isExist(outPath).flatMap {
              case true  => F.pure(())
              case false => writeSingleFile(blocker)
            }
        }

      case SingleOrMulti.Multi =>
        params.sparkOrHadoop match {
          case SparkOrHadoop.Spark =>
            F.delay(TypedDataset.create(rdd).write.mode(params.saveMode).parquet(outPath))
          case SparkOrHadoop.Hadoop =>
            params.saveMode match {
              case SaveMode.Append => F.raiseError(new Exception("append mode is not support"))
              case SaveMode.Overwrite =>
                fileSink[F](blocker).delete(outPath) >> F.delay(writeMultiFiles(ss))
              case SaveMode.ErrorIfExists =>
                fileSink[F](blocker).isExist(outPath).flatMap {
                  case true  => F.raiseError(new Exception(s"$outPath already exist"))
                  case false => F.delay(writeMultiFiles(ss))
                }
              case SaveMode.Ignore =>
                fileSink[F](blocker).isExist(outPath).flatMap {
                  case true  => F.pure(())
                  case false => F.delay(writeMultiFiles(ss))
                }
            }
        }
    }
}
