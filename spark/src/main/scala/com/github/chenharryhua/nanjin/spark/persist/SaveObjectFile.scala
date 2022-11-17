package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.kernel.Sync
import org.apache.spark.rdd.RDD

final class SaveObjectFile[F[_], A](frdd: F[RDD[A]], cfg: HoarderConfig) extends Serializable {

  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveObjectFile[F, A] =
    new SaveObjectFile[F, A](frdd, cfg)

  def overwrite: SaveObjectFile[F, A]      = updateConfig(cfg.overwriteMode)
  def errorIfExists: SaveObjectFile[F, A]  = updateConfig(cfg.errorMode)
  def ignoreIfExists: SaveObjectFile[F, A] = updateConfig(cfg.ignoreMode)

  def run(implicit F: Sync[F]): F[Unit] =
    F.flatMap(frdd) { rdd =>
      new SaveModeAware[F](params.saveMode, params.outPath, rdd.sparkContext.hadoopConfiguration)
        .checkAndRun(F.interruptible(rdd.saveAsObjectFile(params.outPath.pathStr)))
    }
}
