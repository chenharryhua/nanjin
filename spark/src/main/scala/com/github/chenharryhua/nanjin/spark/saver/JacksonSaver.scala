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

final class JacksonSaver[F[_], A](rdd: RDD[A], encoder: Encoder[A], cfg: SaverConfig)
    extends AbstractSaver[F, A](cfg) {

  implicit private val enc: Encoder[A] = encoder

  private def mode(sm: SaveMode): JacksonSaver[F, A] =
    new JacksonSaver(rdd, encoder, cfg.withSaveMode(sm))

  def overwrite: JacksonSaver[F, A]     = mode(SaveMode.Overwrite)
  def errorIfExists: JacksonSaver[F, A] = mode(SaveMode.ErrorIfExists)

  def single: JacksonSaver[F, A] =
    new JacksonSaver[F, A](rdd, encoder, cfg.withSingle)

  def multi: JacksonSaver[F, A] =
    new JacksonSaver[F, A](rdd, encoder, cfg.withMulti)

  override protected def writeSingleFile(rdd: RDD[A], outPath: String, blocker: Blocker)(implicit
    ss: SparkSession,
    F: Concurrent[F],
    cs: ContextShift[F]): F[Unit] =
    rdd.stream[F].through(fileSink[F](blocker).jackson(outPath)).compile.drain

  override protected def writeMultiFiles(rdd: RDD[A], outPath: String, ss: SparkSession): Unit = {
    val job = Job.getInstance(ss.sparkContext.hadoopConfiguration)
    AvroJob.setOutputKeySchema(job, enc.schema)
    ss.sparkContext.hadoopConfiguration.addResource(job.getConfiguration)
    utils.genericRecordPair(rdd, encoder).saveAsNewAPIHadoopFile[NJJacksonKeyOutputFormat](outPath)
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
