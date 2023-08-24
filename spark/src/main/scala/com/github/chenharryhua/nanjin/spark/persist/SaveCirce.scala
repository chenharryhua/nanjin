package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.kernel.Sync
import com.github.chenharryhua.nanjin.terminals.{CirceCompression, NJCompression, NJCompressionLevel}
import io.circe.Encoder as JsonEncoder
import org.apache.spark.rdd.RDD

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

  def append: SaveCirce[F, A]         = updateConfig(cfg.appendMode)
  def overwrite: SaveCirce[F, A]      = updateConfig(cfg.overwriteMode)
  def errorIfExists: SaveCirce[F, A]  = updateConfig(cfg.errorMode)
  def ignoreIfExists: SaveCirce[F, A] = updateConfig(cfg.ignoreMode)

  // def snappy: SaveCirce[F, A] = updateConfig(cfg.outputCompression(NJCompression.Snappy))
  // def zstd(level: Int): SaveCirce[F, A]    = updateConfig(cfg.outputCompression(NJCompression.Zstandard(level)))

  def bzip2: SaveCirce[F, A] = updateConfig(cfg.outputCompression(NJCompression.Bzip2))
  def deflate(level: NJCompressionLevel): SaveCirce[F, A] = updateConfig(
    cfg.outputCompression(NJCompression.Deflate(level)))
  def gzip: SaveCirce[F, A]       = updateConfig(cfg.outputCompression(NJCompression.Gzip))
  def lz4: SaveCirce[F, A]        = updateConfig(cfg.outputCompression(NJCompression.Lz4))
  def uncompress: SaveCirce[F, A] = updateConfig(cfg.outputCompression(NJCompression.Uncompressed))
  def snappy: SaveCirce[F, A]     = updateConfig(cfg.outputCompression(NJCompression.Snappy))

  def withCompression(cc: CirceCompression): SaveCirce[F, A] =
    updateConfig(cfg.outputCompression(cc))

  def run(implicit F: Sync[F]): F[Unit] =
    F.flatMap(frdd) { rdd =>
      new SaveModeAware[F](params.saveMode, params.outPath, rdd.sparkContext.hadoopConfiguration).checkAndRun(
        F.interruptible(saveRDD.circe(rdd, params.outPath, params.compression, isKeepNull)(encoder)))
    }
}
