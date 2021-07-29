package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.kernel.Sync
import com.github.chenharryhua.nanjin.spark.RddExt
import com.sksamuel.avro4s.Encoder as AvroEncoder
import fs2.Stream
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD

final class SaveJackson[F[_], A](rdd: RDD[A], encoder: AvroEncoder[A], cfg: HoarderConfig) extends Serializable {
  def file: SaveSingleJackson[F, A]  = new SaveSingleJackson[F, A](rdd, encoder, cfg)
  def folder: SaveMultiJackson[F, A] = new SaveMultiJackson[F, A](rdd, encoder, cfg)
}

final class SaveSingleJackson[F[_], A](rdd: RDD[A], encoder: AvroEncoder[A], cfg: HoarderConfig) extends Serializable {
  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveSingleJackson[F, A] =
    new SaveSingleJackson[F, A](rdd, encoder, cfg)

  def overwrite: SaveSingleJackson[F, A]      = updateConfig(cfg.overwrite_mode)
  def errorIfExists: SaveSingleJackson[F, A]  = updateConfig(cfg.error_mode)
  def ignoreIfExists: SaveSingleJackson[F, A] = updateConfig(cfg.ignore_mode)

  def gzip: SaveSingleJackson[F, A]                = updateConfig(cfg.output_compression(Compression.Gzip))
  def deflate(level: Int): SaveSingleJackson[F, A] = updateConfig(cfg.output_compression(Compression.Deflate(level)))
  def uncompress: SaveSingleJackson[F, A]          = updateConfig(cfg.output_compression(Compression.Uncompressed))

  def stream(implicit F: Sync[F]): Stream[F, Unit] = {
    val hc: Configuration     = rdd.sparkContext.hadoopConfiguration
    val sma: SaveModeAware[F] = new SaveModeAware[F](params.saveMode, params.outPath, hc)

    sma.checkAndRun(
      rdd.stream[F].through(sinks.jackson(params.outPath, hc, encoder, params.compression.fs2Compression)))
  }
}

final class SaveMultiJackson[F[_], A](rdd: RDD[A], encoder: AvroEncoder[A], cfg: HoarderConfig) extends Serializable {
  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveMultiJackson[F, A] =
    new SaveMultiJackson[F, A](rdd, encoder, cfg)

  def append: SaveMultiJackson[F, A]         = updateConfig(cfg.append_mode)
  def overwrite: SaveMultiJackson[F, A]      = updateConfig(cfg.overwrite_mode)
  def errorIfExists: SaveMultiJackson[F, A]  = updateConfig(cfg.error_mode)
  def ignoreIfExists: SaveMultiJackson[F, A] = updateConfig(cfg.ignore_mode)

  def bzip2: SaveMultiJackson[F, A]               = updateConfig(cfg.output_compression(Compression.Bzip2))
  def gzip: SaveMultiJackson[F, A]                = updateConfig(cfg.output_compression(Compression.Gzip))
  def deflate(level: Int): SaveMultiJackson[F, A] = updateConfig(cfg.output_compression(Compression.Deflate(level)))
  def uncompress: SaveMultiJackson[F, A]          = updateConfig(cfg.output_compression(Compression.Uncompressed))

  def run(implicit F: Sync[F]): F[Unit] =
    new SaveModeAware[F](params.saveMode, params.outPath, rdd.sparkContext.hadoopConfiguration)
      .checkAndRun(F.delay(saveRDD.jackson(rdd, params.outPath, encoder, params.compression)))
}
