package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.{Blocker, ContextShift, Sync}
import com.github.chenharryhua.nanjin.spark.RddExt
import com.sksamuel.avro4s.{Encoder => AvroEncoder}
import org.apache.avro.mapreduce.AvroJob
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.util.CompressionCodecs

final class SaveJackson[F[_], A](rdd: RDD[A], encoder: AvroEncoder[A], cfg: HoarderConfig) extends Serializable {
  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveJackson[F, A] =
    new SaveJackson[F, A](rdd, encoder, cfg)

  def append: SaveJackson[F, A]         = updateConfig(cfg.withAppend)
  def overwrite: SaveJackson[F, A]      = updateConfig(cfg.withOverwrite)
  def errorIfExists: SaveJackson[F, A]  = updateConfig(cfg.withError)
  def ignoreIfExists: SaveJackson[F, A] = updateConfig(cfg.withIgnore)

  def outPath(path: String): SaveJackson[F, A] = updateConfig(cfg.withOutPutPath(path))

  def file: SaveJackson[F, A]   = updateConfig(cfg.withSingleFile)
  def folder: SaveJackson[F, A] = updateConfig(cfg.withFolder)

  def gzip: SaveJackson[F, A]                = updateConfig(cfg.withCompression(Compression.Gzip))
  def deflate(level: Int): SaveJackson[F, A] = updateConfig(cfg.withCompression(Compression.Deflate(level)))
  def uncompress: SaveJackson[F, A]          = updateConfig(cfg.withCompression(Compression.Uncompressed))

  def run(blocker: Blocker)(implicit F: Sync[F], cs: ContextShift[F]): F[Unit] = {
    val hadoopConfiguration   = new Configuration(rdd.sparkContext.hadoopConfiguration)
    val sma: SaveModeAware[F] = new SaveModeAware[F](params.saveMode, params.outPath, hadoopConfiguration)

    params.folderOrFile match {
      case FolderOrFile.SingleFile =>
        sma.checkAndRun(blocker)(
          rdd
            .stream[F]
            .through(
              sinks.jackson(params.outPath, hadoopConfiguration, encoder, params.compression.fs2Compression, blocker))
            .compile
            .drain)

      case FolderOrFile.Folder =>
        val sparkjob = F.delay {
          CompressionCodecs.setCodecConfiguration(
            hadoopConfiguration,
            CompressionCodecs.getCodecClassName(params.compression.name))
          val job = Job.getInstance(hadoopConfiguration)
          AvroJob.setOutputKeySchema(job, encoder.schema)
          rdd.sparkContext.hadoopConfiguration.addResource(job.getConfiguration)
          utils.genericRecordPair(rdd, encoder).saveAsNewAPIHadoopFile[NJJacksonKeyOutputFormat](params.outPath)
        }
        sma.checkAndRun(blocker)(sparkjob)
    }
  }
}
