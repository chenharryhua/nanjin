package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.{Blocker, Concurrent, ContextShift}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD

final class SaveObjectFile[F[_], A](rdd: RDD[A], cfg: HoarderConfig) extends Serializable {

  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveObjectFile[F, A] =
    new SaveObjectFile[F, A](rdd, cfg)

  def overwrite: SaveObjectFile[F, A]      = updateConfig(cfg.withOverwrite)
  def errorIfExists: SaveObjectFile[F, A]  = updateConfig(cfg.withError)
  def ignoreIfExists: SaveObjectFile[F, A] = updateConfig(cfg.withIgnore)

  def outPath(path: String): SaveObjectFile[F, A] = updateConfig(cfg.withOutPutPath(path))

  def run(blocker: Blocker)(implicit F: Concurrent[F], cs: ContextShift[F]): F[Unit] = {
    val hadoopConfiguration = new Configuration(rdd.sparkContext.hadoopConfiguration)

    val sma: SaveModeAware[F] =
      new SaveModeAware[F](params.saveMode, params.outPath, hadoopConfiguration)

    params.compression.ccg[F](hadoopConfiguration)

    sma.checkAndRun(blocker)(F.delay {
      rdd.sparkContext.hadoopConfiguration.addResource(hadoopConfiguration)
      rdd.saveAsObjectFile(params.outPath)
    })
  }
}
