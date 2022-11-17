package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.kernel.Sync
import com.github.chenharryhua.nanjin.terminals.{KantanCompression, NJCompression}
import kantan.csv.{CsvConfiguration, HeaderEncoder}
import org.apache.spark.rdd.RDD

final class SaveKantanCsv[F[_], A](
  frdd: F[RDD[A]],
  csvConfiguration: CsvConfiguration,
  cfg: HoarderConfig,
  encoder: HeaderEncoder[A])
    extends Serializable {

  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveKantanCsv[F, A] =
    new SaveKantanCsv[F, A](frdd, csvConfiguration, cfg, encoder)

  def append: SaveKantanCsv[F, A]         = updateConfig(cfg.appendMode)
  def overwrite: SaveKantanCsv[F, A]      = updateConfig(cfg.overwriteMode)
  def errorIfExists: SaveKantanCsv[F, A]  = updateConfig(cfg.errorMode)
  def ignoreIfExists: SaveKantanCsv[F, A] = updateConfig(cfg.ignoreMode)

  def bzip2: SaveKantanCsv[F, A] = updateConfig(cfg.outputCompression(NJCompression.Bzip2))
  def deflate(level: Int): SaveKantanCsv[F, A] = updateConfig(
    cfg.outputCompression(NJCompression.Deflate(level)))
  def gzip: SaveKantanCsv[F, A]       = updateConfig(cfg.outputCompression(NJCompression.Gzip))
  def lz4: SaveKantanCsv[F, A]        = updateConfig(cfg.outputCompression(NJCompression.Lz4))
  def uncompress: SaveKantanCsv[F, A] = updateConfig(cfg.outputCompression(NJCompression.Uncompressed))
  // def snappy: SaveKantanCsv[F, A]     = updateConfig(cfg.outputCompression(NJCompression.Snappy))

  def withCompression(kc: KantanCompression): SaveKantanCsv[F, A] = updateConfig(cfg.outputCompression(kc))

  def run(implicit F: Sync[F]): F[Unit] =
    F.flatMap(frdd) { rdd =>
      new SaveModeAware[F](params.saveMode, params.outPath, rdd.sparkContext.hadoopConfiguration).checkAndRun(
        F.interruptible(
          saveRDD.kantan[A](rdd, params.outPath, params.compression, csvConfiguration, encoder)))
    }
}
