package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.{Blocker, ContextShift, Sync}
import com.github.chenharryhua.nanjin.devices.NJHadoop
import com.github.chenharryhua.nanjin.pipes.GenericRecordCodec
import com.github.chenharryhua.nanjin.spark.RddExt
import com.sksamuel.avro4s.{Encoder => AvroEncoder}
import frameless.cats.implicits._
import fs2.Pipe
import org.apache.avro.file.CodecFactory
import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapreduce.AvroJob
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD

final class SaveAvro[F[_], A](rdd: RDD[A], encoder: AvroEncoder[A], cfg: HoarderConfig) extends Serializable {

  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveAvro[F, A] =
    new SaveAvro[F, A](rdd, encoder, cfg)

  def overwrite: SaveAvro[F, A]      = updateConfig(cfg.withOverwrite)
  def errorIfExists: SaveAvro[F, A]  = updateConfig(cfg.withError)
  def ignoreIfExists: SaveAvro[F, A] = updateConfig(cfg.withIgnore)

  def outPath(path: String): SaveAvro[F, A] = updateConfig(cfg.withOutPutPath(path))

  def file: SaveAvro[F, A]   = updateConfig(cfg.withSingleFile)
  def folder: SaveAvro[F, A] = updateConfig(cfg.withFolder)

  def deflate(level: Int): SaveAvro[F, A] =
    updateConfig(cfg.withCompression(Compression.Deflate(level)))

  def xz(level: Int): SaveAvro[F, A] =
    updateConfig(cfg.withCompression(Compression.Xz(level)))

  def snappy: SaveAvro[F, A] =
    updateConfig(cfg.withCompression(Compression.Snappy))

  def bzip2: SaveAvro[F, A] =
    updateConfig(cfg.withCompression(Compression.Bzip2))

  def run(blocker: Blocker)(implicit F: Sync[F], cs: ContextShift[F]): F[Unit] = {

    val hadoopConfiguration = new Configuration(rdd.sparkContext.hadoopConfiguration)

    val sma: SaveModeAware[F] =
      new SaveModeAware[F](params.saveMode, params.outPath, hadoopConfiguration)

    val cf: CodecFactory = params.compression.avro(hadoopConfiguration)

    params.folderOrFile match {
      case FolderOrFile.SingleFile =>
        val hadoop: NJHadoop[F]                = NJHadoop[F](hadoopConfiguration, blocker)
        val pipe: Pipe[F, A, GenericRecord]    = new GenericRecordCodec[F, A].encode(encoder)
        val sink: Pipe[F, GenericRecord, Unit] = hadoop.avroSink(params.outPath, encoder.schema, cf)
        sma.checkAndRun(blocker)(rdd.stream[F].through(pipe).through(sink).compile.drain)
      case FolderOrFile.Folder =>
        val sparkjob = F.delay {
          val job = Job.getInstance(hadoopConfiguration)
          AvroJob.setOutputKeySchema(job, encoder.schema)
          rdd.sparkContext.hadoopConfiguration.addResource(job.getConfiguration)
          utils.genericRecordPair(rdd, encoder).saveAsNewAPIHadoopFile[NJAvroKeyOutputFormat](params.outPath)
        }
        sma.checkAndRun(blocker)(sparkjob)
    }
  }
}
