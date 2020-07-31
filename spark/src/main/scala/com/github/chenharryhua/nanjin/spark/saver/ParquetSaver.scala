package com.github.chenharryhua.nanjin.spark.saver

import cats.effect.{Blocker, ContextShift, Sync}
import cats.implicits._
import com.github.chenharryhua.nanjin.spark.{fileSink, RddExt}
import com.sksamuel.avro4s.{Encoder, SchemaFor}
import frameless.{TypedDataset, TypedEncoder}
import monocle.macros.Lenses
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.mapreduce.Job
import org.apache.parquet.avro.{AvroParquetOutputFormat, GenericDataSupplier}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

@Lenses final case class ParquetSaver[A](
  rdd: RDD[A],
  encoder: Encoder[A],
  outPath: String,
  saveMode: SaveMode,
  singleOrMulti: SingleOrMulti,
  sparkOrHadoop: SparkOrHadoop,
  constraint: TypedEncoder[A]) {

  implicit private val te: TypedEncoder[A] = constraint
  implicit private val enc: Encoder[A]     = encoder

  def withEncoder(enc: Encoder[A]): ParquetSaver[A] =
    ParquetSaver.encoder.set(enc)(this)

  def withSchema(schema: Schema): ParquetSaver[A] = {
    val schemaFor: SchemaFor[A] = SchemaFor[A](schema)
    ParquetSaver.encoder[A].modify(e => e.withSchema(schemaFor))(this)
  }

  def mode(sm: SaveMode): ParquetSaver[A] =
    ParquetSaver.saveMode.set(sm)(this)

  def overwrite: ParquetSaver[A]     = mode(SaveMode.Overwrite)
  def errorIfExists: ParquetSaver[A] = mode(SaveMode.ErrorIfExists)

  def single: ParquetSaver[A] =
    ParquetSaver.singleOrMulti.set(SingleOrMulti.Single)(this)

  def multi: ParquetSaver[A] =
    ParquetSaver.singleOrMulti.set(SingleOrMulti.Multi)(this)

  def spark: ParquetSaver[A] =
    ParquetSaver.sparkOrHadoop.set(SparkOrHadoop.Spark)(this)

  def hadoop: ParquetSaver[A] =
    ParquetSaver.sparkOrHadoop.set(SparkOrHadoop.Hadoop)(this)

  private def writeSingleFile[F[_]](
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

  def run[F[_]](
    blocker: Blocker)(implicit ss: SparkSession, F: Sync[F], cs: ContextShift[F]): F[Unit] =
    singleOrMulti match {
      case SingleOrMulti.Single =>
        saveMode match {
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
        sparkOrHadoop match {
          case SparkOrHadoop.Spark =>
            F.delay(TypedDataset.create(rdd).write.mode(saveMode).parquet(outPath))
          case SparkOrHadoop.Hadoop =>
            saveMode match {
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
