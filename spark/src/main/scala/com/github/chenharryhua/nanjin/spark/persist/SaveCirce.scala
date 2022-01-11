package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.kernel.Sync
import com.github.chenharryhua.nanjin.common.ChunkSize
import com.github.chenharryhua.nanjin.spark.RddExt
import fs2.Stream
import io.circe.Encoder as JsonEncoder
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD

final class SaveCirce[F[_], A](rdd: RDD[A], cfg: HoarderConfig, isKeepNull: Boolean) extends Serializable {
  def keepNull: SaveCirce[F, A] = new SaveCirce[F, A](rdd, cfg, true)
  def dropNull: SaveCirce[F, A] = new SaveCirce[F, A](rdd, cfg, false)

  def file: SaveSingleCirce[F, A]  = new SaveSingleCirce[F, A](rdd, cfg, isKeepNull)
  def folder: SaveMultiCirce[F, A] = new SaveMultiCirce[F, A](rdd, cfg, isKeepNull)
}

final class SaveSingleCirce[F[_], A](rdd: RDD[A], cfg: HoarderConfig, isKeepNull: Boolean) extends Serializable {

  private def updateConfig(cfg: HoarderConfig): SaveSingleCirce[F, A] =
    new SaveSingleCirce[F, A](rdd, cfg, isKeepNull)

  val params: HoarderParams = cfg.evalConfig

  def overwrite: SaveSingleCirce[F, A]      = updateConfig(cfg.overwriteMode)
  def errorIfExists: SaveSingleCirce[F, A]  = updateConfig(cfg.errorMode)
  def ignoreIfExists: SaveSingleCirce[F, A] = updateConfig(cfg.ignoreMode)

  def gzip: SaveSingleCirce[F, A]                = updateConfig(cfg.outputCompression(Compression.Gzip))
  def deflate(level: Int): SaveSingleCirce[F, A] = updateConfig(cfg.outputCompression(Compression.Deflate(level)))
  def uncompress: SaveSingleCirce[F, A]          = updateConfig(cfg.outputCompression(Compression.Uncompressed))

  def withChunkSize(cs: ChunkSize): SaveSingleCirce[F, A] = updateConfig(cfg.chunkSize(cs))

  def sink(implicit F: Sync[F], jsonEncoder: JsonEncoder[A]): Stream[F, Unit] = {
    val hc: Configuration     = rdd.sparkContext.hadoopConfiguration
    val sma: SaveModeAware[F] = new SaveModeAware[F](params.saveMode, params.outPath, hc)
    sma.checkAndRun(
      rdd
        .stream[F](params.chunkSize)
        .through(sinks.circe(params.outPath, hc, isKeepNull, params.compression.fs2Compression)))
  }
}

final class SaveMultiCirce[F[_], A](rdd: RDD[A], cfg: HoarderConfig, isKeepNull: Boolean) extends Serializable {
  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveMultiCirce[F, A] =
    new SaveMultiCirce[F, A](rdd, cfg, isKeepNull)

  def append: SaveMultiCirce[F, A]         = updateConfig(cfg.appendMode)
  def overwrite: SaveMultiCirce[F, A]      = updateConfig(cfg.overwriteMode)
  def errorIfExists: SaveMultiCirce[F, A]  = updateConfig(cfg.errorMode)
  def ignoreIfExists: SaveMultiCirce[F, A] = updateConfig(cfg.ignoreMode)

//  def snappy: SaveMultiCirce[F, A]              = updateConfig(cfg.withCompression(Compression.Snappy))
//  def lz4: SaveMultiCirce[F, A]                 = updateConfig(cfg.withCompression(Compression.Lz4))
  def bzip2: SaveMultiCirce[F, A]               = updateConfig(cfg.outputCompression(Compression.Bzip2))
  def gzip: SaveMultiCirce[F, A]                = updateConfig(cfg.outputCompression(Compression.Gzip))
  def deflate(level: Int): SaveMultiCirce[F, A] = updateConfig(cfg.outputCompression(Compression.Deflate(level)))
  def uncompress: SaveMultiCirce[F, A]          = updateConfig(cfg.outputCompression(Compression.Uncompressed))

  def run(implicit F: Sync[F], je: JsonEncoder[A]): F[Unit] =
    new SaveModeAware[F](params.saveMode, params.outPath, rdd.sparkContext.hadoopConfiguration)
      .checkAndRun(F.interruptibleMany(saveRDD.circe(rdd, params.outPath, params.compression, isKeepNull)))
}
