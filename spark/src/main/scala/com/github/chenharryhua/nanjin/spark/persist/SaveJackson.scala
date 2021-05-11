package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.Sync
import com.github.chenharryhua.nanjin.spark.RddExt
import com.sksamuel.avro4s.{Encoder => AvroEncoder}
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

  def overwrite: SaveSingleJackson[F, A]      = updateConfig(cfg.withOverwrite)
  def errorIfExists: SaveSingleJackson[F, A]  = updateConfig(cfg.withError)
  def ignoreIfExists: SaveSingleJackson[F, A] = updateConfig(cfg.withIgnore)

  def gzip: SaveSingleJackson[F, A]                = updateConfig(cfg.withCompression(Compression.Gzip))
  def deflate(level: Int): SaveSingleJackson[F, A] = updateConfig(cfg.withCompression(Compression.Deflate(level)))
  def uncompress: SaveSingleJackson[F, A]          = updateConfig(cfg.withCompression(Compression.Uncompressed))

  def run(implicit F: Sync[F]): F[Unit] = {
    val hc: Configuration     = rdd.sparkContext.hadoopConfiguration
    val sma: SaveModeAware[F] = new SaveModeAware[F](params.saveMode, params.outPath, hc)

    sma.checkAndRun(
      rdd
        .stream[F]
        .through(sinks.jackson(params.outPath, hc, encoder, params.compression.fs2Compression))
        .compile
        .drain)
  }
}

final class SaveMultiJackson[F[_], A](rdd: RDD[A], encoder: AvroEncoder[A], cfg: HoarderConfig) extends Serializable {
  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveMultiJackson[F, A] =
    new SaveMultiJackson[F, A](rdd, encoder, cfg)

  def append: SaveMultiJackson[F, A]         = updateConfig(cfg.withAppend)
  def overwrite: SaveMultiJackson[F, A]      = updateConfig(cfg.withOverwrite)
  def errorIfExists: SaveMultiJackson[F, A]  = updateConfig(cfg.withError)
  def ignoreIfExists: SaveMultiJackson[F, A] = updateConfig(cfg.withIgnore)

  def bzip2: SaveMultiJackson[F, A]               = updateConfig(cfg.withCompression(Compression.Bzip2))
  def gzip: SaveMultiJackson[F, A]                = updateConfig(cfg.withCompression(Compression.Gzip))
  def deflate(level: Int): SaveMultiJackson[F, A] = updateConfig(cfg.withCompression(Compression.Deflate(level)))
  def uncompress: SaveMultiJackson[F, A]          = updateConfig(cfg.withCompression(Compression.Uncompressed))

  def run(implicit F: Sync[F]): F[Unit] =
    new SaveModeAware[F](params.saveMode, params.outPath, rdd.sparkContext.hadoopConfiguration)
      .checkAndRun(F.delay(saveRDD.jackson(rdd, params.outPath, encoder, params.compression)))
}
