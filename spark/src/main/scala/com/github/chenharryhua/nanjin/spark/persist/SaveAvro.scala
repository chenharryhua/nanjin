package com.github.chenharryhua.nanjin.spark.persist

import cats.data.Kleisli
import cats.effect.kernel.Sync
import com.github.chenharryhua.nanjin.common.ChunkSize
import com.github.chenharryhua.nanjin.spark.RddExt
import com.sksamuel.avro4s.Encoder as AvroEncoder
import fs2.{Pipe, Stream}
import org.apache.avro.file.CodecFactory
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD

final class SaveAvro[F[_], A](rdd: RDD[A], encoder: AvroEncoder[A], cfg: HoarderConfig) extends Serializable {

  private def updateConfig(cfg: HoarderConfig): SaveAvro[F, A] =
    new SaveAvro[F, A](rdd, encoder, cfg)

  def file: SaveSingleAvro[F, A]  = new SaveSingleAvro[F, A](rdd, encoder, cfg, None)
  def folder: SaveMultiAvro[F, A] = new SaveMultiAvro[F, A](rdd, encoder, cfg)

  def deflate(level: Int): SaveAvro[F, A] = updateConfig(cfg.outputCompression(NJCompression.Deflate(level)))
  def xz(level: Int): SaveAvro[F, A]      = updateConfig(cfg.outputCompression(NJCompression.Xz(level)))
  def snappy: SaveAvro[F, A]              = updateConfig(cfg.outputCompression(NJCompression.Snappy))
  def bzip2: SaveAvro[F, A]               = updateConfig(cfg.outputCompression(NJCompression.Bzip2))
  def uncompress: SaveAvro[F, A]          = updateConfig(cfg.outputCompression(NJCompression.Uncompressed))
}

final class SaveSingleAvro[F[_], A](
  rdd: RDD[A],
  encoder: AvroEncoder[A],
  cfg: HoarderConfig,
  listener: Option[Kleisli[F, A, Unit]])
    extends Serializable {
  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveSingleAvro[F, A] =
    new SaveSingleAvro[F, A](rdd, encoder, cfg, listener)

  def overwrite: SaveSingleAvro[F, A]      = updateConfig(cfg.overwriteMode)
  def errorIfExists: SaveSingleAvro[F, A]  = updateConfig(cfg.errorMode)
  def ignoreIfExists: SaveSingleAvro[F, A] = updateConfig(cfg.ignoreMode)

  def withChunkSize(cs: ChunkSize): SaveSingleAvro[F, A] = updateConfig(cfg.chunkSize(cs))
  def withListener(f: A => F[Unit]): SaveSingleAvro[F, A] =
    new SaveSingleAvro[F, A](rdd, encoder, cfg, Some(Kleisli(f)))

  def sink(implicit F: Sync[F]): Stream[F, Unit] = {
    val hc: Configuration     = rdd.sparkContext.hadoopConfiguration
    val sma: SaveModeAware[F] = new SaveModeAware[F](params.saveMode, params.outPath, hc)
    val cf: CodecFactory      = params.compression.avro(hc)
    val src: Stream[F, A]     = rdd.stream[F](params.chunkSize)
    val tgt: Pipe[F, A, Unit] = sinks.avro(params.outPath, hc, encoder, cf)
    val ss: Stream[F, Unit]   = listener.fold(src.through(tgt))(k => src.evalTap(k.run).through(tgt))

    sma.checkAndRun(ss)
  }
}

final class SaveMultiAvro[F[_], A](rdd: RDD[A], encoder: AvroEncoder[A], cfg: HoarderConfig) extends Serializable {
  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveMultiAvro[F, A] = new SaveMultiAvro[F, A](rdd, encoder, cfg)

  def append: SaveMultiAvro[F, A]         = updateConfig(cfg.appendMode)
  def overwrite: SaveMultiAvro[F, A]      = updateConfig(cfg.overwriteMode)
  def errorIfExists: SaveMultiAvro[F, A]  = updateConfig(cfg.errorMode)
  def ignoreIfExists: SaveMultiAvro[F, A] = updateConfig(cfg.ignoreMode)

  def run(implicit F: Sync[F]): F[Unit] =
    new SaveModeAware[F](params.saveMode, params.outPath, rdd.sparkContext.hadoopConfiguration)
      .checkAndRun(F.interruptibleMany(saveRDD.avro(rdd, params.outPath, encoder, params.compression)))
}
