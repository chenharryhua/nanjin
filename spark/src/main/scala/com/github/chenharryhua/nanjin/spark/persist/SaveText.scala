package com.github.chenharryhua.nanjin.spark.persist

import cats.Show
import cats.effect.kernel.Sync
import com.github.chenharryhua.nanjin.spark.RddExt
import fs2.{INothing, Stream}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD

final class SaveText[F[_], A](rdd: RDD[A], cfg: HoarderConfig, suffix: String) extends Serializable {

  def withSuffix(suffix: String): SaveText[F, A] = new SaveText[F, A](rdd, cfg, suffix)

  def file: SaveSingleText[F, A]  = new SaveSingleText(rdd, cfg, suffix)
  def folder: SaveMultiText[F, A] = new SaveMultiText[F, A](rdd, cfg, suffix)

}

final class SaveSingleText[F[_], A](rdd: RDD[A], cfg: HoarderConfig, suffix: String) extends Serializable {
  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveSingleText[F, A] =
    new SaveSingleText[F, A](rdd, cfg, suffix)

  def overwrite: SaveSingleText[F, A]      = updateConfig(cfg.overwriteMode)
  def errorIfExists: SaveSingleText[F, A]  = updateConfig(cfg.errorMode)
  def ignoreIfExists: SaveSingleText[F, A] = updateConfig(cfg.ignoreMode)

  def gzip: SaveSingleText[F, A]                = updateConfig(cfg.outputCompression(Compression.Gzip))
  def deflate(level: Int): SaveSingleText[F, A] = updateConfig(cfg.outputCompression(Compression.Deflate(level)))
  def uncompress: SaveSingleText[F, A]          = updateConfig(cfg.outputCompression(Compression.Uncompressed))

  def sink(implicit F: Sync[F], show: Show[A]): Stream[F, INothing] = {
    val hc: Configuration     = rdd.sparkContext.hadoopConfiguration
    val sma: SaveModeAware[F] = new SaveModeAware[F](params.saveMode, params.outPath, hc)
    sma.checkAndRun(rdd.stream[F].through(sinks.text(params.outPath, hc, params.compression.fs2Compression)))
  }
}

final class SaveMultiText[F[_], A](rdd: RDD[A], cfg: HoarderConfig, suffix: String) extends Serializable {
  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveMultiText[F, A] =
    new SaveMultiText[F, A](rdd, cfg, suffix)

  def append: SaveMultiText[F, A]         = updateConfig(cfg.appendMode)
  def overwrite: SaveMultiText[F, A]      = updateConfig(cfg.overwriteMode)
  def errorIfExists: SaveMultiText[F, A]  = updateConfig(cfg.errorMode)
  def ignoreIfExists: SaveMultiText[F, A] = updateConfig(cfg.ignoreMode)

  def bzip2: SaveMultiText[F, A]               = updateConfig(cfg.outputCompression(Compression.Bzip2))
  def gzip: SaveMultiText[F, A]                = updateConfig(cfg.outputCompression(Compression.Gzip))
  def deflate(level: Int): SaveMultiText[F, A] = updateConfig(cfg.outputCompression(Compression.Deflate(level)))
  def uncompress: SaveMultiText[F, A]          = updateConfig(cfg.outputCompression(Compression.Uncompressed))

  def run(implicit F: Sync[F], show: Show[A]): F[Unit] =
    new SaveModeAware[F](params.saveMode, params.outPath, rdd.sparkContext.hadoopConfiguration)
      .checkAndRun(F.interruptible(many = true)(saveRDD.text(rdd, params.outPath, params.compression, suffix)))
}
