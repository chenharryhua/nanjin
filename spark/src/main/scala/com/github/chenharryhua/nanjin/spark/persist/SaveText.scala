package com.github.chenharryhua.nanjin.spark.persist

import cats.Show
import cats.effect.kernel.Sync
import com.github.chenharryhua.nanjin.terminals.TextCompression
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode

final class SaveText[F[_], A](frdd: F[RDD[A]], cfg: HoarderConfig, show: Show[A], suffix: String)
    extends Serializable with BuildRunnable[F] {

  /** @param suffix:
    *   no leading dot(.)
    * @return
    */
  def withSuffix(suffix: String): SaveText[F, A] = new SaveText[F, A](frdd, cfg, show, suffix)

  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveText[F, A] =
    new SaveText[F, A](frdd, cfg, show, suffix)

  def withSaveMode(sm: SaveMode): SaveText[F, A]                   = updateConfig(cfg.saveMode(sm))
  def withSaveMode(f: NJSaveMode.type => SaveMode): SaveText[F, A] = withSaveMode(f(NJSaveMode))

  def withCompression(tc: TextCompression): SaveText[F, A] = updateConfig(cfg.outputCompression(tc))
  def withCompression(f: TextCompression.type => TextCompression): SaveText[F, A] =
    withCompression(f(TextCompression))

  def run(implicit F: Sync[F]): F[Unit] =
    F.flatMap(frdd) { rdd =>
      new SaveModeAware[F](params.saveMode, params.outPath, rdd.sparkContext.hadoopConfiguration)
        .checkAndRun(F.interruptible(saveRDD.text(rdd, params.outPath, params.compression, suffix)(show)))
    }
}
