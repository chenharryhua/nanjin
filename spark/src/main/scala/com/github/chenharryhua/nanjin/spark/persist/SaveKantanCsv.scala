package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.kernel.Sync
import com.github.chenharryhua.nanjin.terminals.KantanCompression
import kantan.csv.{CsvConfiguration, RowEncoder}
import org.apache.spark.rdd.RDD

final class SaveKantanCsv[F[_], A](
  frdd: F[RDD[A]],
  csvConfiguration: CsvConfiguration,
  cfg: HoarderConfig,
  encoder: RowEncoder[A])
    extends Serializable with BuildRunnable[F] {

  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveKantanCsv[F, A] =
    new SaveKantanCsv[F, A](frdd, csvConfiguration, cfg, encoder)

  def append: SaveKantanCsv[F, A]         = updateConfig(cfg.appendMode)
  def overwrite: SaveKantanCsv[F, A]      = updateConfig(cfg.overwriteMode)
  def errorIfExists: SaveKantanCsv[F, A]  = updateConfig(cfg.errorMode)
  def ignoreIfExists: SaveKantanCsv[F, A] = updateConfig(cfg.ignoreMode)

  def withCompression(kc: KantanCompression): SaveKantanCsv[F, A] = updateConfig(cfg.outputCompression(kc))
  def withCompression(f: KantanCompression.type => KantanCompression): SaveKantanCsv[F, A] =
    withCompression(f(KantanCompression))

  def run(implicit F: Sync[F]): F[Unit] =
    F.flatMap(frdd) { rdd =>
      new SaveModeAware[F](params.saveMode, params.outPath, rdd.sparkContext.hadoopConfiguration).checkAndRun(
        F.interruptible(
          saveRDD.kantan[A](rdd, params.outPath, params.compression, csvConfiguration, encoder)))
    }
}
