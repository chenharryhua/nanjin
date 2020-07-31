package com.github.chenharryhua.nanjin.spark.saver

import cats.effect.{Blocker, ContextShift, Sync}
import cats.implicits._
import com.github.chenharryhua.nanjin.spark.mapreduce.NJAvroKeyOutputFormat
import com.github.chenharryhua.nanjin.spark.{fileSink, RddExt, RddToDataFrame}
import com.sksamuel.avro4s.{Encoder, SchemaFor}
import monocle.macros.Lenses
import org.apache.avro.Schema
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

@Lenses final case class AvroSaver[F[_], A](
  rdd: RDD[A],
  encoder: Encoder[A],
  outPath: String,
  saveMode: SaveMode,
  singleOrMulti: SingleOrMulti,
  sparkOrHadoop: SparkOrHadoop) {
  implicit private val enc: Encoder[A] = encoder

  def withEncoder(enc: Encoder[A]): AvroSaver[F, A] =
    AvroSaver.encoder.set(enc)(this)

  def withSchema(schema: Schema): AvroSaver[F, A] = {
    val schemaFor: SchemaFor[A] = SchemaFor[A](schema)
    AvroSaver.encoder[F, A].modify(e => e.withSchema(schemaFor))(this)
  }

  def mode(sm: SaveMode): AvroSaver[F, A] =
    AvroSaver.saveMode.set(sm)(this)

  def overwrite: AvroSaver[F, A]     = mode(SaveMode.Overwrite)
  def errorIfExists: AvroSaver[F, A] = mode(SaveMode.ErrorIfExists)

  def single: AvroSaver[F, A] =
    AvroSaver.singleOrMulti.set(SingleOrMulti.Single)(this)

  def multi: AvroSaver[F, A] =
    AvroSaver.singleOrMulti.set(SingleOrMulti.Multi)(this)

  def spark: AvroSaver[F, A] =
    AvroSaver.sparkOrHadoop.set(SparkOrHadoop.Spark)(this)

  def hadoop: AvroSaver[F, A] =
    AvroSaver.sparkOrHadoop.set(SparkOrHadoop.Hadoop)(this)

  private def writeSingleFile(
    blocker: Blocker)(implicit ss: SparkSession, F: Sync[F], cs: ContextShift[F]): F[Unit] =
    rdd.stream[F].through(fileSink[F](blocker).avro(outPath)).compile.drain

  private def writeMultiFiles(ss: SparkSession): Unit =
    utils.genericRecordPair(rdd, encoder, ss).saveAsNewAPIHadoopFile[NJAvroKeyOutputFormat](outPath)

  def run(blocker: Blocker)(implicit ss: SparkSession, F: Sync[F], cs: ContextShift[F]): F[Unit] =
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
            F.delay {
              new RddToDataFrame[A](rdd, encoder, ss).toDF.write
                .mode(saveMode)
                .format("avro")
                .save(outPath)
            }
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
