package com.github.chenharryhua.nanjin.spark.persist

import cats.Show
import cats.effect.kernel.Sync
import com.github.chenharryhua.nanjin.terminals.TextCompression
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode

final class SaveText[A](rdd: RDD[A], cfg: HoarderConfig, show: Show[A], suffix: String)
    extends Serializable with BuildRunnable {

  /** @param suffix:
    *   no leading dot(.)
    * @return
    */
  def withSuffix(suffix: String): SaveText[A] = new SaveText[A](rdd, cfg, show, suffix)

  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveText[A] =
    new SaveText[A](rdd, cfg, show, suffix)

  def withSaveMode(sm: SaveMode): SaveText[A] = updateConfig(cfg.saveMode(sm))
  def withSaveMode(f: SparkSaveMode.type => SaveMode): SaveText[A] = withSaveMode(f(SparkSaveMode))

  def withCompression(tc: TextCompression): SaveText[A] = updateConfig(cfg.outputCompression(tc))
  def withCompression(f: TextCompression.type => TextCompression): SaveText[A] =
    withCompression(f(TextCompression))

  def run[F[_]](implicit F: Sync[F]): F[Unit] =
    new SaveModeAware[F](params.saveMode, params.outPath, rdd.sparkContext.hadoopConfiguration)
      .checkAndRun(F.interruptible(saveRDD.text(rdd, params.outPath, params.compression, suffix)(show)))

}
