package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.kernel.Sync
import org.apache.spark.rdd.RDD

final class SaveObjectFile[F[_], A](rdd: RDD[A], cfg: HoarderConfig) extends Serializable {

  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveObjectFile[F, A] =
    new SaveObjectFile[F, A](rdd, cfg)

  def overwrite: SaveObjectFile[F, A]      = updateConfig(cfg.overwrite_mode)
  def errorIfExists: SaveObjectFile[F, A]  = updateConfig(cfg.error_mode)
  def ignoreIfExists: SaveObjectFile[F, A] = updateConfig(cfg.ignore_mode)

  def run(implicit F: Sync[F]): F[Unit] =
    new SaveModeAware[F](params.saveMode, params.outPath, rdd.sparkContext.hadoopConfiguration)
      .checkAndRun(F.delay(rdd.saveAsObjectFile(params.outPath)))
}
