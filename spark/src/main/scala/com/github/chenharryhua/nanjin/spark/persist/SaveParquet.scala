package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.kernel.Sync
import com.sksamuel.avro4s.Encoder as AvroEncoder
import org.apache.spark.sql.Dataset

final class SaveParquet[F[_], A](ds: Dataset[A], cfg: HoarderConfig) extends Serializable {

  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveParquet[F, A] =
    new SaveParquet[F, A](ds, cfg)

  def append: SaveParquet[F, A]         = updateConfig(cfg.appendMode)
  def overwrite: SaveParquet[F, A]      = updateConfig(cfg.overwriteMode)
  def errorIfExists: SaveParquet[F, A]  = updateConfig(cfg.errorMode)
  def ignoreIfExists: SaveParquet[F, A] = updateConfig(cfg.ignoreMode)

  def gzip: SaveParquet[F, A]             = updateConfig(cfg.outputCompression(NJCompression.Gzip))
  def lz4: SaveParquet[F, A]              = updateConfig(cfg.outputCompression(NJCompression.Lz4))
  def snappy: SaveParquet[F, A]           = updateConfig(cfg.outputCompression(NJCompression.Snappy))
  def uncompress: SaveParquet[F, A]       = updateConfig(cfg.outputCompression(NJCompression.Uncompressed))
  def zstd(level: Int): SaveParquet[F, A] = updateConfig(cfg.outputCompression(NJCompression.Zstandard(level)))

  def run(implicit F: Sync[F]): F[Unit] =
    new SaveModeAware[F](params.saveMode, params.outPath, ds.sparkSession.sparkContext.hadoopConfiguration)
      .checkAndRun(F.interruptibleMany {
        ds.write.option("compression", params.compression.name).mode(params.saveMode).parquet(params.outPath.pathStr)
      })
}
