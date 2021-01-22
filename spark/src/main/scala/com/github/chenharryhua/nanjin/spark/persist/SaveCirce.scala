package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.{Blocker, ContextShift, Sync}
import com.github.chenharryhua.nanjin.spark.RddExt
import io.circe.{Encoder => JsonEncoder}
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

  def overwrite: SaveSingleCirce[F, A]      = updateConfig(cfg.withOverwrite)
  def errorIfExists: SaveSingleCirce[F, A]  = updateConfig(cfg.withError)
  def ignoreIfExists: SaveSingleCirce[F, A] = updateConfig(cfg.withIgnore)

  def gzip: SaveSingleCirce[F, A]                = updateConfig(cfg.withCompression(Compression.Gzip))
  def deflate(level: Int): SaveSingleCirce[F, A] = updateConfig(cfg.withCompression(Compression.Deflate(level)))
  def uncompress: SaveSingleCirce[F, A]          = updateConfig(cfg.withCompression(Compression.Uncompressed))

  def run(blocker: Blocker)(implicit F: Sync[F], cs: ContextShift[F], jsonEncoder: JsonEncoder[A]): F[Unit] = {
    val hc: Configuration     = rdd.sparkContext.hadoopConfiguration
    val sma: SaveModeAware[F] = new SaveModeAware[F](params.saveMode, params.outPath, hc)
    sma.checkAndRun(blocker)(
      rdd
        .stream[F]
        .through(sinks.circe(params.outPath, hc, isKeepNull, params.compression.fs2Compression, blocker))
        .compile
        .drain)
  }
}

final class SaveMultiCirce[F[_], A](rdd: RDD[A], cfg: HoarderConfig, isKeepNull: Boolean) extends Serializable {
  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveMultiCirce[F, A] =
    new SaveMultiCirce[F, A](rdd, cfg, isKeepNull)

  def append: SaveMultiCirce[F, A]         = updateConfig(cfg.withAppend)
  def overwrite: SaveMultiCirce[F, A]      = updateConfig(cfg.withOverwrite)
  def errorIfExists: SaveMultiCirce[F, A]  = updateConfig(cfg.withError)
  def ignoreIfExists: SaveMultiCirce[F, A] = updateConfig(cfg.withIgnore)

//  def snappy: SaveMultiCirce[F, A]              = updateConfig(cfg.withCompression(Compression.Snappy))
//  def lz4: SaveMultiCirce[F, A]                 = updateConfig(cfg.withCompression(Compression.Lz4))
  def bzip2: SaveMultiCirce[F, A]               = updateConfig(cfg.withCompression(Compression.Bzip2))
  def gzip: SaveMultiCirce[F, A]                = updateConfig(cfg.withCompression(Compression.Gzip))
  def deflate(level: Int): SaveMultiCirce[F, A] = updateConfig(cfg.withCompression(Compression.Deflate(level)))
  def uncompress: SaveMultiCirce[F, A]          = updateConfig(cfg.withCompression(Compression.Uncompressed))

  def run(blocker: Blocker)(implicit F: Sync[F], cs: ContextShift[F], je: JsonEncoder[A]): F[Unit] =
    new SaveModeAware[F](params.saveMode, params.outPath, rdd.sparkContext.hadoopConfiguration)
      .checkAndRun(blocker)(F.delay(saveRDD.circe(rdd, params.outPath, params.compression, isKeepNull)))
}
