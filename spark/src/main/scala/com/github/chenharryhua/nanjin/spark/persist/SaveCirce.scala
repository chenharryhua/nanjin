package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.kernel.Sync
import io.circe.Encoder as JsonEncoder
import org.apache.spark.rdd.RDD

final class SaveCirce[F[_], A](rdd: RDD[A], cfg: HoarderConfig, isKeepNull: Boolean) extends Serializable {
  def keepNull: SaveCirce[F, A] = new SaveCirce[F, A](rdd, cfg, true)
  def dropNull: SaveCirce[F, A] = new SaveCirce[F, A](rdd, cfg, false)

  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveCirce[F, A] =
    new SaveCirce[F, A](rdd, cfg, isKeepNull)

  def append: SaveCirce[F, A]         = updateConfig(cfg.appendMode)
  def overwrite: SaveCirce[F, A]      = updateConfig(cfg.overwriteMode)
  def errorIfExists: SaveCirce[F, A]  = updateConfig(cfg.errorMode)
  def ignoreIfExists: SaveCirce[F, A] = updateConfig(cfg.ignoreMode)

//  def snappy: SaveMultiCirce[F, A]              = updateConfig(cfg.withCompression(Compression.Snappy))
//  def lz4: SaveMultiCirce[F, A]                 = updateConfig(cfg.withCompression(Compression.Lz4))
  def bzip2: SaveCirce[F, A]               = updateConfig(cfg.outputCompression(NJCompression.Bzip2))
  def gzip: SaveCirce[F, A]                = updateConfig(cfg.outputCompression(NJCompression.Gzip))
  def deflate(level: Int): SaveCirce[F, A] = updateConfig(cfg.outputCompression(NJCompression.Deflate(level)))
  def uncompress: SaveCirce[F, A]          = updateConfig(cfg.outputCompression(NJCompression.Uncompressed))

  def run(implicit F: Sync[F], je: JsonEncoder[A]): F[Unit] =
    new SaveModeAware[F](params.saveMode, params.outPath, rdd.sparkContext.hadoopConfiguration)
      .checkAndRun(F.interruptibleMany(saveRDD.circe(rdd, params.outPath, params.compression, isKeepNull)))
}
