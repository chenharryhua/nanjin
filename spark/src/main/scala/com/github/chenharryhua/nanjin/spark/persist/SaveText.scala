package com.github.chenharryhua.nanjin.spark.persist

import cats.Show
import cats.effect.kernel.Sync
import com.github.chenharryhua.nanjin.common.{NJCompression, TextCompression}
import org.apache.spark.rdd.RDD

final class SaveText[F[_], A](val rdd: RDD[A], cfg: HoarderConfig, suffix: String) extends Serializable {

  def withSuffix(suffix: String): SaveText[F, A] = new SaveText[F, A](rdd, cfg, suffix)

  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveText[F, A] =
    new SaveText[F, A](rdd, cfg, suffix)

  def append: SaveText[F, A]         = updateConfig(cfg.appendMode)
  def overwrite: SaveText[F, A]      = updateConfig(cfg.overwriteMode)
  def errorIfExists: SaveText[F, A]  = updateConfig(cfg.errorMode)
  def ignoreIfExists: SaveText[F, A] = updateConfig(cfg.ignoreMode)

  def bzip2: SaveText[F, A]               = updateConfig(cfg.outputCompression(NJCompression.Bzip2))
  def deflate(level: Int): SaveText[F, A] = updateConfig(cfg.outputCompression(NJCompression.Deflate(level)))
  def gzip: SaveText[F, A]                = updateConfig(cfg.outputCompression(NJCompression.Gzip))
  def lz4: SaveText[F, A]                 = updateConfig(cfg.outputCompression(NJCompression.Lz4))
  def uncompress: SaveText[F, A]          = updateConfig(cfg.outputCompression(NJCompression.Uncompressed))
  def snappy: SaveText[F, A]              = updateConfig(cfg.outputCompression(NJCompression.Snappy))

  def withCompression(tc: TextCompression): SaveText[F, A] = updateConfig(cfg.outputCompression(tc))

  def run(implicit F: Sync[F], show: Show[A]): F[Unit] =
    new SaveModeAware[F](params.saveMode, params.outPath, rdd.sparkContext.hadoopConfiguration)
      .checkAndRun(F.interruptibleMany(saveRDD.text(rdd, params.outPath, params.compression, suffix)))
}
