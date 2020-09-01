package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.{Blocker, Concurrent, ContextShift}
import cats.implicits._
import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import com.github.chenharryhua.nanjin.spark.mapreduce.NJAvroKeyOutputFormat
import com.github.chenharryhua.nanjin.spark.{fileSink, utils, RddExt}
import com.sksamuel.avro4s.{Encoder => AvroEncoder}
import frameless.cats.implicits._
import org.apache.avro.mapreduce.AvroJob
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.reflect.ClassTag

final class SaveAvro[F[_], A: ClassTag](rdd: RDD[A], cfg: HoarderConfig)(implicit
  codec: NJAvroCodec[A],
  ss: SparkSession)
    extends Serializable {

  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveAvro[F, A] =
    new SaveAvro[F, A](rdd, cfg)

  def overwrite: SaveAvro[F, A]      = updateConfig(cfg.withOverwrite)
  def errorIfExists: SaveAvro[F, A]  = updateConfig(cfg.withError)
  def ignoreIfExists: SaveAvro[F, A] = updateConfig(cfg.withIgnore)

  def spark: SaveAvro[F, A] = updateConfig(cfg.withSpark)
  def raw: SaveAvro[F, A]   = updateConfig(cfg.withRaw)

  def single: SaveAvro[F, A] = updateConfig(cfg.withSingle)
  def multi: SaveAvro[F, A]  = updateConfig(cfg.withMulti)

  def run(blocker: Blocker)(implicit F: Concurrent[F], cs: ContextShift[F]): F[Unit] = {
    implicit val encoder: AvroEncoder[A] = codec.avroEncoder

    val sma: SaveModeAware[F] = new SaveModeAware[F](params.saveMode, params.outPath, ss)

    (params.singleOrMulti, params.sparkOrRaw) match {
      case (SingleOrMulti.Single, _) =>
        sma.checkAndRun(blocker)(
          rdd.stream[F].through(fileSink[F](blocker).avro(params.outPath)).compile.drain)
      case (SingleOrMulti.Multi, SparkOrRaw.Spark) =>
        sma.checkAndRun(blocker)(
          F.delay(
            utils
              .normalizedDF(rdd, codec.avroEncoder)
              .write
              .mode(SaveMode.Overwrite)
              .format("avro")
              .save(params.outPath))
        )
      case (SingleOrMulti.Multi, SparkOrRaw.Raw) =>
        val sparkjob = F.delay {
          val job = Job.getInstance(ss.sparkContext.hadoopConfiguration)
          AvroJob.setOutputKeySchema(job, codec.schema)
          ss.sparkContext.hadoopConfiguration.addResource(job.getConfiguration)
          utils
            .genericRecordPair(rdd.map(codec.idConversion), codec.avroEncoder)
            .saveAsNewAPIHadoopFile[NJAvroKeyOutputFormat](params.outPath)
        }
        sma.checkAndRun(blocker)(sparkjob)
    }
  }
}
