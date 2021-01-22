package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.{Blocker, ContextShift, Sync}
import org.apache.spark.rdd.RDD

final class SaveObjectFile[F[_], A](rdd: RDD[A], cfg: HoarderConfig) extends Serializable {

  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveObjectFile[F, A] =
    new SaveObjectFile[F, A](rdd, cfg)

  def overwrite: SaveObjectFile[F, A]      = updateConfig(cfg.withOverwrite)
  def errorIfExists: SaveObjectFile[F, A]  = updateConfig(cfg.withError)
  def ignoreIfExists: SaveObjectFile[F, A] = updateConfig(cfg.withIgnore)

  def run(blocker: Blocker)(implicit F: Sync[F], cs: ContextShift[F]): F[Unit] = {
    val sma: SaveModeAware[F] =
      new SaveModeAware[F](params.saveMode, params.outPath, rdd.sparkContext.hadoopConfiguration)
    sma.checkAndRun(blocker)(F.delay(rdd.saveAsObjectFile(params.outPath)))
  }
}
