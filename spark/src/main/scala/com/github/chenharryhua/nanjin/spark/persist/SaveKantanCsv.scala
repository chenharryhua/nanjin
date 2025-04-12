package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.kernel.Sync
import com.github.chenharryhua.nanjin.terminals.KantanCompression
import kantan.csv.{CsvConfiguration, RowEncoder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode

final class SaveKantanCsv[A](
  rdd: RDD[A],
  csvConfiguration: CsvConfiguration,
  cfg: HoarderConfig,
  encoder: RowEncoder[A])
    extends Serializable with BuildRunnable {

  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveKantanCsv[A] =
    new SaveKantanCsv[A](rdd, csvConfiguration, cfg, encoder)

  def withSaveMode(sm: SaveMode): SaveKantanCsv[A]                      = updateConfig(cfg.saveMode(sm))
  def withSaveMode(f: SparkSaveMode.type => SaveMode): SaveKantanCsv[A] = withSaveMode(f(SparkSaveMode))

  def withCompression(kc: KantanCompression): SaveKantanCsv[A] = updateConfig(cfg.outputCompression(kc))
  def withCompression(f: KantanCompression.type => KantanCompression): SaveKantanCsv[A] =
    withCompression(f(KantanCompression))

  def run[F[_]](implicit F: Sync[F]): F[Unit] =
    new SaveModeAware[F](params.saveMode, params.outPath, rdd.sparkContext.hadoopConfiguration).checkAndRun(
      F.interruptible(saveRDD.kantan[A](rdd, params.outPath, params.compression, csvConfiguration, encoder)))

}
