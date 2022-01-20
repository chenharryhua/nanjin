package com.github.chenharryhua.nanjin.spark.persist

import cats.data.Kleisli
import cats.effect.kernel.Sync
import com.github.chenharryhua.nanjin.common.ChunkSize
import com.github.chenharryhua.nanjin.spark.RddExt
import com.sksamuel.avro4s.Encoder as AvroEncoder
import fs2.{Pipe, Stream}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD

final class SaveJackson[F[_], A](rdd: RDD[A], encoder: AvroEncoder[A], cfg: HoarderConfig) extends Serializable {
  def file: SaveSingleJackson[F, A] =
    new SaveSingleJackson[F, A](rdd, encoder, cfg, None)
  def folder: SaveMultiJackson[F, A] = new SaveMultiJackson[F, A](rdd, encoder, cfg)
}

final class SaveSingleJackson[F[_], A](
  rdd: RDD[A],
  encoder: AvroEncoder[A],
  cfg: HoarderConfig,
  listener: Option[Kleisli[F, A, Unit]])
    extends Serializable {
  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveSingleJackson[F, A] =
    new SaveSingleJackson[F, A](rdd, encoder, cfg, listener)

  def overwrite: SaveSingleJackson[F, A]      = updateConfig(cfg.overwriteMode)
  def errorIfExists: SaveSingleJackson[F, A]  = updateConfig(cfg.errorMode)
  def ignoreIfExists: SaveSingleJackson[F, A] = updateConfig(cfg.ignoreMode)

  def gzip: SaveSingleJackson[F, A]                = updateConfig(cfg.outputCompression(Compression.Gzip))
  def deflate(level: Int): SaveSingleJackson[F, A] = updateConfig(cfg.outputCompression(Compression.Deflate(level)))
  def uncompress: SaveSingleJackson[F, A]          = updateConfig(cfg.outputCompression(Compression.Uncompressed))

  def withChunkSize(cs: ChunkSize): SaveSingleJackson[F, A] = updateConfig(cfg.chunkSize(cs))
  def withListener(f: A => F[Unit]): SaveSingleJackson[F, A] =
    new SaveSingleJackson[F, A](rdd, encoder, cfg, Some(Kleisli(f)))

  def sink(implicit F: Sync[F]): Stream[F, Unit] = {
    val hc: Configuration     = rdd.sparkContext.hadoopConfiguration
    val sma: SaveModeAware[F] = new SaveModeAware[F](params.saveMode, params.outPath, hc)

    val src: Stream[F, A] = rdd.stream[F](params.chunkSize)
    val tgt: Pipe[F, A, Unit] =
      sinks.jackson(params.outPath, hc, encoder, params.compression.fs2Compression, params.chunkSize)
    val ss: Stream[F, Unit] = listener.fold(src.through(tgt))(k => src.evalTap(k.run).through(tgt))

    sma.checkAndRun(ss)
  }
}

final class SaveMultiJackson[F[_], A](rdd: RDD[A], encoder: AvroEncoder[A], cfg: HoarderConfig) extends Serializable {
  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveMultiJackson[F, A] =
    new SaveMultiJackson[F, A](rdd, encoder, cfg)

  def append: SaveMultiJackson[F, A]         = updateConfig(cfg.appendMode)
  def overwrite: SaveMultiJackson[F, A]      = updateConfig(cfg.overwriteMode)
  def errorIfExists: SaveMultiJackson[F, A]  = updateConfig(cfg.errorMode)
  def ignoreIfExists: SaveMultiJackson[F, A] = updateConfig(cfg.ignoreMode)

  def bzip2: SaveMultiJackson[F, A]               = updateConfig(cfg.outputCompression(Compression.Bzip2))
  def gzip: SaveMultiJackson[F, A]                = updateConfig(cfg.outputCompression(Compression.Gzip))
  def deflate(level: Int): SaveMultiJackson[F, A] = updateConfig(cfg.outputCompression(Compression.Deflate(level)))
  def uncompress: SaveMultiJackson[F, A]          = updateConfig(cfg.outputCompression(Compression.Uncompressed))

  def run(implicit F: Sync[F]): F[Unit] =
    new SaveModeAware[F](params.saveMode, params.outPath, rdd.sparkContext.hadoopConfiguration)
      .checkAndRun(F.interruptibleMany(saveRDD.jackson(rdd, params.outPath, encoder, params.compression)))
}
