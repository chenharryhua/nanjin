package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.kernel.Sync
import com.github.chenharryhua.nanjin.terminals.toHadoopPath
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode

final class SaveObjectFile[A](rdd: RDD[A], cfg: HoarderConfig) extends Serializable with BuildRunnable {

  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveObjectFile[A] =
    new SaveObjectFile[A](rdd, cfg)

  def withSaveMode(sm: SaveMode): SaveObjectFile[A] = updateConfig(cfg.saveMode(sm))
  def withSaveMode(f: SparkSaveMode.type => SaveMode): SaveObjectFile[A] = withSaveMode(f(SparkSaveMode))

  override def run[F[_]](implicit F: Sync[F]): F[Unit] =
    internalRun(
      sparkContext = rdd.sparkContext,
      params = params,
      job = F.delay(rdd.saveAsObjectFile(toHadoopPath(params.outPath).toString)),
      description = None)

  override def run[F[_]](description: String)(implicit F: Sync[F]): F[Unit] =
    internalRun(
      sparkContext = rdd.sparkContext,
      params = params,
      job = F.delay(rdd.saveAsObjectFile(toHadoopPath(params.outPath).toString)),
      description = Some(description))

}
