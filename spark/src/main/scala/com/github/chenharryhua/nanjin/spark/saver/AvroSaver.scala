package com.github.chenharryhua.nanjin.spark.saver

import cats.effect.{Blocker, ContextShift, Sync}
import cats.implicits._
import com.github.chenharryhua.nanjin.spark.mapreduce.NJAvroKeyOutputFormat
import com.github.chenharryhua.nanjin.spark.{fileSink, utils, RddExt}
import com.sksamuel.avro4s.{Encoder, SchemaFor}
import org.apache.avro.Schema
import org.apache.avro.mapreduce.AvroJob
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

final class AvroSaver[F[_], A](rdd: RDD[A], encoder: Encoder[A], outPath: String, cfg: SaverConfig)
    extends Serializable {
  implicit private val enc: Encoder[A] = encoder

  val params: SaverParams = cfg.evalConfig

  def withEncoder(enc: Encoder[A]): AvroSaver[F, A] =
    new AvroSaver[F, A](rdd, enc, outPath, cfg)

  def withSchema(schema: Schema): AvroSaver[F, A] = {
    val schemaFor: SchemaFor[A] = SchemaFor[A](schema)
    new AvroSaver[F, A](rdd, encoder.withSchema(schemaFor), outPath, cfg)
  }

  def mode(sm: SaveMode): AvroSaver[F, A] =
    new AvroSaver[F, A](rdd, encoder, outPath, cfg.withSaveMode(sm))

  def overwrite: AvroSaver[F, A]     = mode(SaveMode.Overwrite)
  def errorIfExists: AvroSaver[F, A] = mode(SaveMode.ErrorIfExists)

  def single: AvroSaver[F, A] =
    new AvroSaver[F, A](rdd, encoder, outPath, cfg.withSingle)

  def multi: AvroSaver[F, A] =
    new AvroSaver[F, A](rdd, encoder, outPath, cfg.withMulti)

  def spark: AvroSaver[F, A] =
    new AvroSaver[F, A](rdd, encoder, outPath, cfg.withSpark)

  def hadoop: AvroSaver[F, A] =
    new AvroSaver[F, A](rdd, encoder, outPath, cfg.withHadoop)

  private def writeSingleFile(
    blocker: Blocker)(implicit ss: SparkSession, F: Sync[F], cs: ContextShift[F]): F[Unit] =
    rdd.stream[F].through(fileSink[F](blocker).avro(outPath)).compile.drain

  private def writeMultiFiles(ss: SparkSession): Unit = {
    val job = Job.getInstance(ss.sparkContext.hadoopConfiguration)
    AvroJob.setOutputKeySchema(job, enc.schema)
    ss.sparkContext.hadoopConfiguration.addResource(job.getConfiguration)
    utils.genericRecordPair(rdd, encoder).saveAsNewAPIHadoopFile[NJAvroKeyOutputFormat](outPath)
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
            F.delay(rdd.toDF.write.mode(params.saveMode).format("avro").save(outPath))
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
