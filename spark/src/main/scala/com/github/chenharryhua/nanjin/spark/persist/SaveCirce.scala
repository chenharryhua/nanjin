package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.kernel.Sync
import com.github.chenharryhua.nanjin.terminals.CirceCompression
import io.circe.Encoder as JsonEncoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode

final class SaveCirce[A](rdd: RDD[A], cfg: HoarderConfig, isKeepNull: Boolean, encoder: JsonEncoder[A])
    extends Serializable with BuildRunnable {
  def keepNull: SaveCirce[A] = new SaveCirce[A](rdd, cfg, true, encoder)
  def dropNull: SaveCirce[A] = new SaveCirce[A](rdd, cfg, false, encoder)

  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveCirce[A] =
    new SaveCirce[A](rdd, cfg, isKeepNull, encoder)

  def withSaveMode(sm: SaveMode): SaveCirce[A]                      = updateConfig(cfg.saveMode(sm))
  def withSaveMode(f: SparkSaveMode.type => SaveMode): SaveCirce[A] = withSaveMode(f(SparkSaveMode))

  def withCompression(cc: CirceCompression): SaveCirce[A] =
    updateConfig(cfg.outputCompression(cc))
  def withCompression(f: CirceCompression.type => CirceCompression): SaveCirce[A] =
    withCompression(f(CirceCompression))

  def run[F[_]](implicit F: Sync[F]): F[Unit] =
    new SaveModeAware[F](params.saveMode, params.outPath, rdd.sparkContext.hadoopConfiguration).checkAndRun(
      F.interruptible(saveRDD.circe(rdd, params.outPath, params.compression, isKeepNull)(encoder)))

  def runWithCount[F[_]](implicit F: Sync[F]): F[Long] =
    F.map(run[F])(_ => rdd.count())
}
