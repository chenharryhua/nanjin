package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.kernel.Sync
import com.github.chenharryhua.nanjin.terminals.{NJCompression, NJCompressionLevel, ParquetCompression}
import com.sksamuel.avro4s.Encoder as AvroEncoder
import org.apache.spark.rdd.RDD
final class SaveParquet[F[_], A](frdd: F[RDD[A]], encoder: AvroEncoder[A], cfg: HoarderConfig)
    extends Serializable {

  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveParquet[F, A] =
    new SaveParquet[F, A](frdd, encoder, cfg)

  def append: SaveParquet[F, A]         = updateConfig(cfg.appendMode)
  def overwrite: SaveParquet[F, A]      = updateConfig(cfg.overwriteMode)
  def errorIfExists: SaveParquet[F, A]  = updateConfig(cfg.errorMode)
  def ignoreIfExists: SaveParquet[F, A] = updateConfig(cfg.ignoreMode)

//  def brotli: SaveParquet[F, A] = updateConfig(cfg.outputCompression(NJCompression.Brotli))
//  def lzo: SaveParquet[F, A]        = updateConfig(cfg.outputCompression(NJCompression.Lzo))
  def gzip: SaveParquet[F, A]       = updateConfig(cfg.outputCompression(NJCompression.Gzip))
  def lz4: SaveParquet[F, A]        = updateConfig(cfg.outputCompression(NJCompression.Lz4))
  def lz4raw: SaveParquet[F, A]     = updateConfig(cfg.outputCompression(NJCompression.Lz4_Raw))
  def snappy: SaveParquet[F, A]     = updateConfig(cfg.outputCompression(NJCompression.Snappy))
  def uncompress: SaveParquet[F, A] = updateConfig(cfg.outputCompression(NJCompression.Uncompressed))
  def zstd(level: NJCompressionLevel): SaveParquet[F, A] =
    updateConfig(cfg.outputCompression(NJCompression.Zstandard(level)))

  def withCompression(pc: ParquetCompression): SaveParquet[F, A] = updateConfig(cfg.outputCompression(pc))

  def run(implicit F: Sync[F]): F[Unit] =
    F.flatMap(frdd) { rdd =>
      new SaveModeAware[F](params.saveMode, params.outPath, rdd.sparkContext.hadoopConfiguration)
        .checkAndRun(F.interruptible(saveRDD.parquet(rdd, params.outPath, encoder, params.compression)))
    }
}
