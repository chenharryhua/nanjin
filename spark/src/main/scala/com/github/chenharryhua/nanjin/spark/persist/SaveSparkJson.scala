package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.kernel.Sync
import org.apache.spark.sql.Dataset

final class SaveSparkJson[F[_], A](ds: Dataset[A], cfg: HoarderConfig, isKeepNull: Boolean) extends Serializable {

  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveSparkJson[F, A] =
    new SaveSparkJson[F, A](ds, cfg, isKeepNull)

  def append: SaveSparkJson[F, A]         = updateConfig(cfg.append_mode)
  def overwrite: SaveSparkJson[F, A]      = updateConfig(cfg.overwrite_mode)
  def errorIfExists: SaveSparkJson[F, A]  = updateConfig(cfg.error_mode)
  def ignoreIfExists: SaveSparkJson[F, A] = updateConfig(cfg.ignore_mode)

  def gzip: SaveSparkJson[F, A]                = updateConfig(cfg.output_compression(Compression.Gzip))
  def deflate(level: Int): SaveSparkJson[F, A] = updateConfig(cfg.output_compression(Compression.Deflate(level)))
  def bzip2: SaveSparkJson[F, A]               = updateConfig(cfg.output_compression(Compression.Bzip2))
  def uncompress: SaveSparkJson[F, A]          = updateConfig(cfg.output_compression(Compression.Uncompressed))

  def keepNull: SaveSparkJson[F, A] = new SaveSparkJson[F, A](ds, cfg, true)
  def dropNull: SaveSparkJson[F, A] = new SaveSparkJson[F, A](ds, cfg, false)

  def run(implicit F: Sync[F]): F[Unit] =
    new SaveModeAware[F](params.saveMode, params.outPath, ds.sparkSession.sparkContext.hadoopConfiguration)
      .checkAndRun(F.delay {
        ds.write
          .mode(params.saveMode)
          .option("compression", params.compression.name)
          .option("ignoreNullFields", !isKeepNull)
          .json(params.outPath)
      })
}
