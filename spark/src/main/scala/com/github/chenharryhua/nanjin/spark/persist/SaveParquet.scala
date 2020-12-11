package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.{Blocker, Concurrent, ContextShift}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

final class SaveParquet[F[_], A](ds: Dataset[A], cfg: HoarderConfig) extends Serializable {

  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveParquet[F, A] =
    new SaveParquet[F, A](ds, cfg)

  def overwrite: SaveParquet[F, A]      = updateConfig(cfg.withOverwrite)
  def errorIfExists: SaveParquet[F, A]  = updateConfig(cfg.withError)
  def ignoreIfExists: SaveParquet[F, A] = updateConfig(cfg.withIgnore)

  def outPath(path: String): SaveParquet[F, A] = updateConfig(cfg.withOutPutPath(path))

  def snappy: SaveParquet[F, A] =
    updateConfig(cfg.withCompression(Compression.Snappy))

  def gzip: SaveParquet[F, A] =
    updateConfig(cfg.withCompression(Compression.Gzip))

  def run(blocker: Blocker)(implicit F: Concurrent[F], cs: ContextShift[F]): F[Unit] = {
    val ss: SparkSession = ds.sparkSession

    val sma: SaveModeAware[F]         = new SaveModeAware[F](params.saveMode, params.outPath, ss)
    val ccg: CompressionCodecGroup[F] = params.compression.ccg(ss.sparkContext.hadoopConfiguration)
    sma.checkAndRun(blocker)(
      F.delay(
        ds.write.option("compression", ccg.name).mode(SaveMode.Overwrite).parquet(params.outPath)))
  }
}
