package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.kernel.Sync
import com.github.chenharryhua.nanjin.spark.RddExt
import com.sksamuel.avro4s.Encoder as AvroEncoder
import frameless.cats.implicits.*
import fs2.Stream
import org.apache.avro.file.CodecFactory
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD

final class SaveAvro[F[_], A](rdd: RDD[A], encoder: AvroEncoder[A], cfg: HoarderConfig) extends Serializable {

  private def updateConfig(cfg: HoarderConfig): SaveAvro[F, A] =
    new SaveAvro[F, A](rdd, encoder, cfg)

  def file: SaveSingleAvro[F, A]  = new SaveSingleAvro[F, A](rdd, encoder, cfg)
  def folder: SaveMultiAvro[F, A] = new SaveMultiAvro[F, A](rdd, encoder, cfg)

  def deflate(level: Int): SaveAvro[F, A] = updateConfig(cfg.outputCompression(Compression.Deflate(level)))
  def xz(level: Int): SaveAvro[F, A]      = updateConfig(cfg.outputCompression(Compression.Xz(level)))
  def snappy: SaveAvro[F, A]              = updateConfig(cfg.outputCompression(Compression.Snappy))
  def bzip2: SaveAvro[F, A]               = updateConfig(cfg.outputCompression(Compression.Bzip2))
  def uncompress: SaveAvro[F, A]          = updateConfig(cfg.outputCompression(Compression.Uncompressed))
}

final class SaveSingleAvro[F[_], A](rdd: RDD[A], encoder: AvroEncoder[A], cfg: HoarderConfig) extends Serializable {
  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveSingleAvro[F, A] =
    new SaveSingleAvro[F, A](rdd, encoder, cfg)

  def overwrite: SaveSingleAvro[F, A]      = updateConfig(cfg.overwriteMode)
  def errorIfExists: SaveSingleAvro[F, A]  = updateConfig(cfg.errorMode)
  def ignoreIfExists: SaveSingleAvro[F, A] = updateConfig(cfg.ignoreMode)

  def stream(implicit F: Sync[F]): Stream[F, Unit] = {
    val hc: Configuration     = rdd.sparkContext.hadoopConfiguration
    val sma: SaveModeAware[F] = new SaveModeAware[F](params.saveMode, params.outPath, hc)
    val cf: CodecFactory      = params.compression.avro(hc)
    sma.checkAndRun(rdd.stream[F].through(sinks.avro(params.outPath, hc, encoder, cf)))
  }
}

final class SaveMultiAvro[F[_], A](rdd: RDD[A], encoder: AvroEncoder[A], cfg: HoarderConfig) extends Serializable {
  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveMultiAvro[F, A] =
    new SaveMultiAvro[F, A](rdd, encoder, cfg)

  def append: SaveMultiAvro[F, A]         = updateConfig(cfg.appendMode)
  def overwrite: SaveMultiAvro[F, A]      = updateConfig(cfg.overwriteMode)
  def errorIfExists: SaveMultiAvro[F, A]  = updateConfig(cfg.errorMode)
  def ignoreIfExists: SaveMultiAvro[F, A] = updateConfig(cfg.ignoreMode)

  def run(implicit F: Sync[F]): F[Unit] =
    new SaveModeAware[F](params.saveMode, params.outPath, rdd.sparkContext.hadoopConfiguration)
      .checkAndRun(F.interruptible(many = true)(saveRDD.avro(rdd, params.outPath, encoder, params.compression)))
}
