package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.{Blocker, ContextShift, Sync}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{Dataset, SaveMode}

final class SaveSparkJson[F[_], A](ds: Dataset[A], cfg: HoarderConfig, isKeepNull: Boolean) extends Serializable {

  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveSparkJson[F, A] =
    new SaveSparkJson[F, A](ds, cfg, isKeepNull)

  def append: SaveSparkJson[F, A]         = updateConfig(cfg.withAppend)
  def overwrite: SaveSparkJson[F, A]      = updateConfig(cfg.withOverwrite)
  def errorIfExists: SaveSparkJson[F, A]  = updateConfig(cfg.withError)
  def ignoreIfExists: SaveSparkJson[F, A] = updateConfig(cfg.withIgnore)

  def outPath(path: String): SaveSparkJson[F, A] = updateConfig(cfg.withOutPutPath(path))

  def gzip: SaveSparkJson[F, A]                = updateConfig(cfg.withCompression(Compression.Gzip))
  def deflate(level: Int): SaveSparkJson[F, A] = updateConfig(cfg.withCompression(Compression.Deflate(level)))
  def bzip2: SaveSparkJson[F, A]               = updateConfig(cfg.withCompression(Compression.Bzip2))
  def uncompress: SaveSparkJson[F, A]          = updateConfig(cfg.withCompression(Compression.Uncompressed))

  def keepNull: SaveSparkJson[F, A] = new SaveSparkJson[F, A](ds, cfg, true)
  def dropNull: SaveSparkJson[F, A] = new SaveSparkJson[F, A](ds, cfg, false)

  def run(blocker: Blocker)(implicit F: Sync[F], cs: ContextShift[F]): F[Unit] = {
    val hadoopConfiguration = new Configuration(ds.sparkSession.sparkContext.hadoopConfiguration)

    val sma: SaveModeAware[F] =
      new SaveModeAware[F](params.saveMode, params.outPath, hadoopConfiguration)

    val ccg: CompressionCodecGroup[F] =
      params.compression.ccg[F](hadoopConfiguration)

    sma.checkAndRun(blocker)(F.delay {
      ds.sparkSession.sparkContext.hadoopConfiguration.addResource(hadoopConfiguration)
      ds.write
        .mode(params.saveMode)
        .option("compression", ccg.name)
        .option("ignoreNullFields", !isKeepNull)
        .json(params.outPath)
    })
  }
}
