package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.{Blocker, Concurrent, ContextShift}
import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import com.github.chenharryhua.nanjin.spark.mapreduce.NJJacksonKeyOutputFormat
import com.github.chenharryhua.nanjin.spark.{fileSink, utils, RddExt}
import com.sksamuel.avro4s.{Encoder => AvroEncoder}
import org.apache.avro.mapreduce.AvroJob
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

final class SaveJackson[F[_], A: ClassTag](rdd: RDD[A], outPath: String, cfg: HoarderConfig)(
  implicit
  codec: NJAvroCodec[A],
  ss: SparkSession)
    extends Serializable {
  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveJackson[F, A] =
    new SaveJackson[F, A](rdd, outPath, cfg)

  def single: SaveJackson[F, A] = updateConfig(cfg.withSingle)
  def multi: SaveJackson[F, A]  = updateConfig(cfg.withMulti)

  def run(blocker: Blocker)(implicit F: Concurrent[F], cs: ContextShift[F]): F[Unit] = {
    implicit val encoder: AvroEncoder[A] = codec.avroEncoder
    val sma: SaveModeAware[F]            = new SaveModeAware[F](params.saveMode, outPath, ss)
    params.singleOrMulti match {
      case SingleOrMulti.Single =>
        sma.checkAndRun(blocker)(
          rdd
            .map(codec.idConversion)
            .stream[F]
            .through(fileSink[F](blocker).jackson(outPath))
            .compile
            .drain)
      case SingleOrMulti.Multi =>
        val sparkjob = F.delay {
          val job = Job.getInstance(ss.sparkContext.hadoopConfiguration)
          AvroJob.setOutputKeySchema(job, codec.schema)
          ss.sparkContext.hadoopConfiguration.addResource(job.getConfiguration)
          utils
            .genericRecordPair(rdd.map(codec.idConversion), codec.avroEncoder)
            .saveAsNewAPIHadoopFile[NJJacksonKeyOutputFormat](outPath)
        }
        sma.checkAndRun(blocker)(sparkjob)
    }
  }
}
