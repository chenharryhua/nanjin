package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.{Blocker, ContextShift, Sync}
import com.github.chenharryhua.nanjin.devices.NJHadoop
import com.github.chenharryhua.nanjin.pipes.{GenericRecordCodec, JacksonSerialization}
import com.github.chenharryhua.nanjin.spark.RddExt
import com.sksamuel.avro4s.{Encoder => AvroEncoder}
import fs2.Pipe
import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapreduce.AvroJob
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD

final class SaveJackson[F[_], A](rdd: RDD[A], encoder: AvroEncoder[A], cfg: HoarderConfig) extends Serializable {
  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveJackson[F, A] =
    new SaveJackson[F, A](rdd, encoder, cfg)

  def overwrite: SaveJackson[F, A]      = updateConfig(cfg.withOverwrite)
  def errorIfExists: SaveJackson[F, A]  = updateConfig(cfg.withError)
  def ignoreIfExists: SaveJackson[F, A] = updateConfig(cfg.withIgnore)

  def outPath(path: String): SaveJackson[F, A] = updateConfig(cfg.withOutPutPath(path))

  def file: SaveJackson[F, A]   = updateConfig(cfg.withSingleFile)
  def folder: SaveJackson[F, A] = updateConfig(cfg.withFolder)

  def gzip: SaveJackson[F, A] = updateConfig(cfg.withCompression(Compression.Gzip))

  def deflate(level: Int): SaveJackson[F, A] =
    updateConfig(cfg.withCompression(Compression.Deflate(level)))

  def run(blocker: Blocker)(implicit F: Sync[F], cs: ContextShift[F]): F[Unit] = {

    val hadoopConfiguration = new Configuration(rdd.sparkContext.hadoopConfiguration)

    val sma: SaveModeAware[F] =
      new SaveModeAware[F](params.saveMode, params.outPath, hadoopConfiguration)

    val ccg: CompressionCodecGroup[F] = params.compression.ccg[F](hadoopConfiguration)

    params.folderOrFile match {
      case FolderOrFile.SingleFile =>
        val hadoop: NJHadoop[F]                = NJHadoop[F](hadoopConfiguration, blocker)
        val gr: Pipe[F, A, GenericRecord]      = new GenericRecordCodec[F, A].encode(encoder)
        val pipe: Pipe[F, GenericRecord, Byte] = new JacksonSerialization[F](encoder.schema).serialize
        val sink: Pipe[F, Byte, Unit]          = hadoop.byteSink(params.outPath)
        sma.checkAndRun(blocker)(
          rdd.stream[F].through(gr).through(pipe).through(ccg.compressionPipe).through(sink).compile.drain)

      case FolderOrFile.Folder =>
        val sparkjob = F.delay {
          val job = Job.getInstance(hadoopConfiguration)
          AvroJob.setOutputKeySchema(job, encoder.schema)
          rdd.sparkContext.hadoopConfiguration.addResource(job.getConfiguration)
          utils.genericRecordPair(rdd, encoder).saveAsNewAPIHadoopFile[NJJacksonKeyOutputFormat](params.outPath)
        }
        sma.checkAndRun(blocker)(sparkjob)
    }
  }
}
