package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.{Blocker, ContextShift, Sync}
import com.github.chenharryhua.nanjin.spark.RddExt
import com.sksamuel.avro4s.{Encoder => AvroEncoder}
import frameless.cats.implicits._
import org.apache.avro.file.CodecFactory
import org.apache.avro.mapreduce.AvroJob
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD

final class SaveAvro[F[_], A](rdd: RDD[A], encoder: AvroEncoder[A], cfg: HoarderConfig) extends Serializable {

  private def updateConfig(cfg: HoarderConfig): SaveAvro[F, A] =
    new SaveAvro[F, A](rdd, encoder, cfg)

  def file: SaveSingleAvro[F, A]  = new SaveSingleAvro[F, A](rdd, encoder, cfg)
  def folder: SaveMultiAvro[F, A] = new SaveMultiAvro[F, A](rdd, encoder, cfg)

  def deflate(level: Int): SaveAvro[F, A] = updateConfig(cfg.withCompression(Compression.Deflate(level)))
  def xz(level: Int): SaveAvro[F, A]      = updateConfig(cfg.withCompression(Compression.Xz(level)))
  def snappy: SaveAvro[F, A]              = updateConfig(cfg.withCompression(Compression.Snappy))
  def bzip2: SaveAvro[F, A]               = updateConfig(cfg.withCompression(Compression.Bzip2))
  def uncompress: SaveAvro[F, A]          = updateConfig(cfg.withCompression(Compression.Uncompressed))
}

final class SaveSingleAvro[F[_], A](rdd: RDD[A], encoder: AvroEncoder[A], cfg: HoarderConfig) extends Serializable {
  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveSingleAvro[F, A] =
    new SaveSingleAvro[F, A](rdd, encoder, cfg)

  def overwrite: SaveSingleAvro[F, A]      = updateConfig(cfg.withOverwrite)
  def errorIfExists: SaveSingleAvro[F, A]  = updateConfig(cfg.withError)
  def ignoreIfExists: SaveSingleAvro[F, A] = updateConfig(cfg.withIgnore)

  def run(blocker: Blocker)(implicit F: Sync[F], cs: ContextShift[F]): F[Unit] = {
    val hadoopConfiguration   = new Configuration(rdd.sparkContext.hadoopConfiguration)
    val sma: SaveModeAware[F] = new SaveModeAware[F](params.saveMode, params.outPath, hadoopConfiguration)
    val cf: CodecFactory      = params.compression.avro(hadoopConfiguration)
    sma.checkAndRun(blocker)(
      rdd.stream[F].through(sinks.avro(params.outPath, hadoopConfiguration, encoder, cf, blocker)).compile.drain)
  }
}

final class SaveMultiAvro[F[_], A](rdd: RDD[A], encoder: AvroEncoder[A], cfg: HoarderConfig) extends Serializable {
  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveMultiAvro[F, A] =
    new SaveMultiAvro[F, A](rdd, encoder, cfg)

  def append: SaveMultiAvro[F, A]         = updateConfig(cfg.withAppend)
  def overwrite: SaveMultiAvro[F, A]      = updateConfig(cfg.withOverwrite)
  def errorIfExists: SaveMultiAvro[F, A]  = updateConfig(cfg.withError)
  def ignoreIfExists: SaveMultiAvro[F, A] = updateConfig(cfg.withIgnore)

  def run(blocker: Blocker)(implicit F: Sync[F], cs: ContextShift[F]): F[Unit] = {
    val hadoopConfiguration   = new Configuration(rdd.sparkContext.hadoopConfiguration)
    val sma: SaveModeAware[F] = new SaveModeAware[F](params.saveMode, params.outPath, hadoopConfiguration)

    sma.checkAndRun(blocker)(F.delay {
      params.compression.avro(hadoopConfiguration)
      val job = Job.getInstance(hadoopConfiguration)
      AvroJob.setOutputKeySchema(job, encoder.schema)
      rdd.sparkContext.hadoopConfiguration.addResource(job.getConfiguration)
      utils.genericRecordPair(rdd, encoder).saveAsNewAPIHadoopFile[NJAvroKeyOutputFormat](params.outPath)
    })
  }
}
