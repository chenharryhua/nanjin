package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.{Blocker, ContextShift, Sync}
import com.github.chenharryhua.nanjin.spark.RddExt
import com.sksamuel.avro4s.{Encoder => AvroEncoder}
import frameless.cats.implicits._
import org.apache.avro.file.CodecFactory
import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.AvroJob
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.NullWritable
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
    val hc: Configuration     = rdd.sparkContext.hadoopConfiguration
    val sma: SaveModeAware[F] = new SaveModeAware[F](params.saveMode, params.outPath, hc)
    val cf: CodecFactory      = params.compression.avro(hc)
    sma.checkAndRun(blocker)(rdd.stream[F].through(sinks.avro(params.outPath, hc, encoder, cf, blocker)).compile.drain)
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
    val config: Configuration = new Configuration(rdd.sparkContext.hadoopConfiguration)
    val sma: SaveModeAware[F] = new SaveModeAware[F](params.saveMode, params.outPath, config)

    sma.checkAndRun(blocker)(F.delay {
      params.compression.avro(config)
      val job = Job.getInstance(config)
      AvroJob.setOutputKeySchema(job, encoder.schema)
      utils
        .genericRecordPair(rdd, encoder)
        .saveAsNewAPIHadoopFile(
          params.outPath,
          classOf[AvroKey[GenericRecord]],
          classOf[NullWritable],
          classOf[NJAvroKeyOutputFormat],
          job.getConfiguration)
    })
  }
}
