package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.{Blocker, Concurrent, ContextShift}
import cats.{Eq, Parallel}
import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.github.chenharryhua.nanjin.spark.{fileSink, RddExt}
import com.sksamuel.avro4s.{Encoder => AvroEncoder}
import org.apache.avro.mapreduce.AvroJob
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

final class SaveJackson[F[_], A: ClassTag](rdd: RDD[A], cfg: HoarderConfig)(implicit
  codec: AvroCodec[A],
  ss: SparkSession)
    extends Serializable {
  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveJackson[F, A] =
    new SaveJackson[F, A](rdd, cfg)

  def file: SaveJackson[F, A]   = updateConfig(cfg.withSingleFile)
  def folder: SaveJackson[F, A] = updateConfig(cfg.withFolder)

  def run(blocker: Blocker)(implicit F: Concurrent[F], cs: ContextShift[F]): F[Unit] = {
    implicit val encoder: AvroEncoder[A] = codec.avroEncoder
    val sma: SaveModeAware[F]            = new SaveModeAware[F](params.saveMode, params.outPath, ss)
    params.folderOrFile match {
      case FolderOrFile.SingleFile =>
        sma.checkAndRun(blocker)(
          rdd
            .map(codec.idConversion)
            .stream[F]
            .through(fileSink[F](blocker).jackson(params.outPath))
            .compile
            .drain)
      case FolderOrFile.Folder =>
        val sparkjob = F.delay {
          val job = Job.getInstance(ss.sparkContext.hadoopConfiguration)
          AvroJob.setOutputKeySchema(job, codec.schema)
          ss.sparkContext.hadoopConfiguration.addResource(job.getConfiguration)
          utils
            .genericRecordPair(rdd.map(codec.idConversion), codec.avroEncoder)
            .saveAsNewAPIHadoopFile[NJJacksonKeyOutputFormat](params.outPath)
        }
        sma.checkAndRun(blocker)(sparkjob)
    }
  }
}

final class PartitionJackson[F[_], A: ClassTag, K: ClassTag: Eq](
  rdd: RDD[A],
  cfg: HoarderConfig,
  bucketing: A => Option[K],
  pathBuilder: (NJFileFormat, K) => String)(implicit codec: AvroCodec[A], ss: SparkSession)
    extends AbstractPartition[F, A, K] {

  val params: HoarderParams = cfg.evalConfig

  def run(
    blocker: Blocker)(implicit F: Concurrent[F], CS: ContextShift[F], P: Parallel[F]): F[Unit] =
    savePartition(
      blocker,
      rdd,
      params.parallelism,
      params.format,
      bucketing,
      pathBuilder,
      (r, p) => new SaveJackson[F, A](r, cfg.withOutPutPath(p)).run(blocker))
}
