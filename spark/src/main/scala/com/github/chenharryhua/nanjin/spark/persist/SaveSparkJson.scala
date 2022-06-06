package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.kernel.Sync
import com.github.chenharryhua.nanjin.terminals.{NJCompression, SparkJsonCompression}
import org.apache.spark.sql.Dataset

final class SaveSparkJson[F[_], A](val dataset: Dataset[A], cfg: HoarderConfig, isKeepNull: Boolean)
    extends Serializable {

  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveSparkJson[F, A] =
    new SaveSparkJson[F, A](dataset, cfg, isKeepNull)

  def append: SaveSparkJson[F, A]         = updateConfig(cfg.appendMode)
  def overwrite: SaveSparkJson[F, A]      = updateConfig(cfg.overwriteMode)
  def errorIfExists: SaveSparkJson[F, A]  = updateConfig(cfg.errorMode)
  def ignoreIfExists: SaveSparkJson[F, A] = updateConfig(cfg.ignoreMode)

  def gzip: SaveSparkJson[F, A] = updateConfig(cfg.outputCompression(NJCompression.Gzip))
  def deflate(level: Int): SaveSparkJson[F, A] = updateConfig(
    cfg.outputCompression(NJCompression.Deflate(level)))
  def bzip2: SaveSparkJson[F, A]      = updateConfig(cfg.outputCompression(NJCompression.Bzip2))
  def uncompress: SaveSparkJson[F, A] = updateConfig(cfg.outputCompression(NJCompression.Uncompressed))

  def withCompression(sc: SparkJsonCompression): SaveSparkJson[F, A] = updateConfig(cfg.outputCompression(sc))

  def keepNull: SaveSparkJson[F, A] = new SaveSparkJson[F, A](dataset, cfg, true)
  def dropNull: SaveSparkJson[F, A] = new SaveSparkJson[F, A](dataset, cfg, false)

  def run(implicit F: Sync[F]): F[Unit] =
    new SaveModeAware[F](
      params.saveMode,
      params.outPath,
      dataset.sparkSession.sparkContext.hadoopConfiguration).checkAndRun(F.interruptibleMany {
      dataset.write
        .mode(params.saveMode)
        .option("compression", params.compression.shortName)
        .option("ignoreNullFields", !isKeepNull)
        .json(params.outPath.pathStr)
    })
}
