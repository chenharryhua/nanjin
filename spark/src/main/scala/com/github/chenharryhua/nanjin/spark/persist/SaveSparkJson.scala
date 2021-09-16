package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.kernel.Sync
import org.apache.spark.sql.Dataset

final class SaveSparkJson[F[_], A](ds: Dataset[A], cfg: HoarderConfig, isKeepNull: Boolean) extends Serializable {

  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveSparkJson[F, A] =
    new SaveSparkJson[F, A](ds, cfg, isKeepNull)

  def append: SaveSparkJson[F, A]         = updateConfig(cfg.appendMode)
  def overwrite: SaveSparkJson[F, A]      = updateConfig(cfg.overwriteMode)
  def errorIfExists: SaveSparkJson[F, A]  = updateConfig(cfg.errorMode)
  def ignoreIfExists: SaveSparkJson[F, A] = updateConfig(cfg.ignoreMode)

  def gzip: SaveSparkJson[F, A]                = updateConfig(cfg.outputCompression(Compression.Gzip))
  def deflate(level: Int): SaveSparkJson[F, A] = updateConfig(cfg.outputCompression(Compression.Deflate(level)))
  def bzip2: SaveSparkJson[F, A]               = updateConfig(cfg.outputCompression(Compression.Bzip2))
  def uncompress: SaveSparkJson[F, A]          = updateConfig(cfg.outputCompression(Compression.Uncompressed))

  def keepNull: SaveSparkJson[F, A] = new SaveSparkJson[F, A](ds, cfg, true)
  def dropNull: SaveSparkJson[F, A] = new SaveSparkJson[F, A](ds, cfg, false)

  def run(implicit F: Sync[F]): F[Unit] =
    new SaveModeAware[F](params.saveMode, params.outPath, ds.sparkSession.sparkContext.hadoopConfiguration)
      .checkAndRun(F.interruptible(many = true) {
        ds.write
          .mode(params.saveMode)
          .option("compression", params.compression.name)
          .option("ignoreNullFields", !isKeepNull)
          .json(params.outPath)
      })
}
