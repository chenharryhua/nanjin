package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.kernel.Sync
import com.github.chenharryhua.nanjin.terminals.CirceCompression
import io.circe.Encoder as JsonEncoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode

final class SaveCirce[F[_], A](
  frdd: F[RDD[A]],
  cfg: HoarderConfig,
  isKeepNull: Boolean,
  encoder: JsonEncoder[A])
    extends Serializable with BuildRunnable[F] {
  def keepNull: SaveCirce[F, A] = new SaveCirce[F, A](frdd, cfg, true, encoder)
  def dropNull: SaveCirce[F, A] = new SaveCirce[F, A](frdd, cfg, false, encoder)

  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveCirce[F, A] =
    new SaveCirce[F, A](frdd, cfg, isKeepNull, encoder)

  def withSaveMode(sm: SaveMode): SaveCirce[F, A]                   = updateConfig(cfg.saveMode(sm))
  def withSaveMode(f: NJSaveMode.type => SaveMode): SaveCirce[F, A] = withSaveMode(f(NJSaveMode))

  def withCompression(cc: CirceCompression): SaveCirce[F, A] =
    updateConfig(cfg.outputCompression(cc))
  def withCompression(f: CirceCompression.type => CirceCompression): SaveCirce[F, A] =
    withCompression(f(CirceCompression))

  def run(implicit F: Sync[F]): F[Unit] =
    F.flatMap(frdd) { rdd =>
      new SaveModeAware[F](params.saveMode, params.outPath, rdd.sparkContext.hadoopConfiguration).checkAndRun(
        F.interruptible(saveRDD.circe(rdd, params.outPath, params.compression, isKeepNull)(encoder)))
    }
}
