package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.kernel.Sync
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode

final class SaveObjectFile[F[_], A](frdd: F[RDD[A]], cfg: HoarderConfig)
    extends Serializable with BuildRunnable[F] {

  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveObjectFile[F, A] =
    new SaveObjectFile[F, A](frdd, cfg)

  def withSaveMode(sm: SaveMode): SaveObjectFile[F, A]                   = updateConfig(cfg.saveMode(sm))
  def withSaveMode(f: NJSaveMode.type => SaveMode): SaveObjectFile[F, A] = withSaveMode(f(NJSaveMode))

  def run(implicit F: Sync[F]): F[Unit] =
    F.flatMap(frdd) { rdd =>
      new SaveModeAware[F](params.saveMode, params.outPath, rdd.sparkContext.hadoopConfiguration)
        .checkAndRun(F.interruptible(rdd.saveAsObjectFile(params.outPath.pathStr)))
    }
}
