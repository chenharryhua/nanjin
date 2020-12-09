package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.{Blocker, Concurrent, ContextShift}
import com.github.chenharryhua.nanjin.devices.NJHadoop
import com.github.chenharryhua.nanjin.pipes.{GenericRecordCodec, JacksonSerialization}
import com.github.chenharryhua.nanjin.spark.RddExt
import com.sksamuel.avro4s.{Encoder => AvroEncoder}
import org.apache.avro.mapreduce.AvroJob
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

final class SaveJackson[F[_], A](rdd: RDD[A], encoder: AvroEncoder[A], cfg: HoarderConfig)
    extends Serializable {
  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveJackson[F, A] =
    new SaveJackson[F, A](rdd, encoder, cfg)

  def overwrite: SaveJackson[F, A]      = updateConfig(cfg.withOverwrite)
  def errorIfExists: SaveJackson[F, A]  = updateConfig(cfg.withError)
  def ignoreIfExists: SaveJackson[F, A] = updateConfig(cfg.withIgnore)

  def file: SaveJackson[F, A]   = updateConfig(cfg.withSingleFile)
  def folder: SaveJackson[F, A] = updateConfig(cfg.withFolder)

  def gzip: SaveJackson[F, A] = updateConfig(cfg.withCompression(Compression.Gzip))

  def deflate(level: Int): SaveJackson[F, A] =
    updateConfig(cfg.withCompression(Compression.Deflate(level)))

  def run(blocker: Blocker)(implicit
    F: Concurrent[F],
    cs: ContextShift[F],
    ss: SparkSession,
    tag: ClassTag[A]): F[Unit] = {
    val sma: SaveModeAware[F] = new SaveModeAware[F](params.saveMode, params.outPath, ss)
    val ccg: CompressionCodecGroup[F] =
      params.compression.ccg[F](ss.sparkContext.hadoopConfiguration)
    params.folderOrFile match {
      case FolderOrFile.SingleFile =>
        val hadoop = NJHadoop[F](ss.sparkContext.hadoopConfiguration, blocker)
        val gr     = new GenericRecordCodec[F, A]
        val pipe   = new JacksonSerialization[F](encoder.schema)
        sma.checkAndRun(blocker)(
          rdd
            .stream[F]
            .through(gr.encode(encoder))
            .through(pipe.serialize)
            .through(ccg.compressionPipe)
            .through(hadoop.byteSink(params.outPath))
            .compile
            .drain)
      case FolderOrFile.Folder =>
        val sparkjob = F.delay {
          val job = Job.getInstance(ss.sparkContext.hadoopConfiguration)
          AvroJob.setOutputKeySchema(job, encoder.schema)
          ss.sparkContext.hadoopConfiguration.addResource(job.getConfiguration)
          utils
            .genericRecordPair(rdd, encoder)
            .saveAsNewAPIHadoopFile[NJJacksonKeyOutputFormat](params.outPath)
        }
        sma.checkAndRun(blocker)(sparkjob)
    }
  }
}
