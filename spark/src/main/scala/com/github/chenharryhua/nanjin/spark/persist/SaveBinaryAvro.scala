package com.github.chenharryhua.nanjin.spark.persist

import cats.data.Kleisli
import cats.effect.kernel.Sync
import com.github.chenharryhua.nanjin.common.ChunkSize
import com.github.chenharryhua.nanjin.spark.RddExt
import com.sksamuel.avro4s.Encoder as AvroEncoder
import fs2.{Pipe, Stream}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD

final class SaveBinaryAvro[F[_], A](rdd: RDD[A], encoder: AvroEncoder[A], cfg: HoarderConfig) extends Serializable {
  def file: SaveSingleBinaryAvro[F, A]  = new SaveSingleBinaryAvro[F, A](rdd, encoder, cfg, None)
  def folder: SaveMultiBinaryAvro[F, A] = new SaveMultiBinaryAvro[F, A](rdd, encoder, cfg)
}

final class SaveSingleBinaryAvro[F[_], A](
  rdd: RDD[A],
  encoder: AvroEncoder[A],
  cfg: HoarderConfig,
  listener: Option[Kleisli[F, A, Unit]])
    extends Serializable {
  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveSingleBinaryAvro[F, A] =
    new SaveSingleBinaryAvro[F, A](rdd, encoder, cfg, listener)

  def overwrite: SaveSingleBinaryAvro[F, A]      = updateConfig(cfg.overwriteMode)
  def errorIfExists: SaveSingleBinaryAvro[F, A]  = updateConfig(cfg.errorMode)
  def ignoreIfExists: SaveSingleBinaryAvro[F, A] = updateConfig(cfg.ignoreMode)

  def withChunkSize(cs: ChunkSize): SaveSingleBinaryAvro[F, A] = updateConfig(cfg.chunkSize(cs))
  def withListener(f: A => F[Unit]): SaveSingleBinaryAvro[F, A] =
    new SaveSingleBinaryAvro[F, A](rdd, encoder, cfg, Some(Kleisli(f)))

  def sink(implicit F: Sync[F]): Stream[F, Unit] = {
    val hc: Configuration     = rdd.sparkContext.hadoopConfiguration
    val sma: SaveModeAware[F] = new SaveModeAware[F](params.saveMode, params.outPath, hc)
    val src: Stream[F, A]     = rdd.stream[F](params.chunkSize)
    val tgt: Pipe[F, A, Unit] = sinks.binAvro(params.outPath, hc, encoder, params.chunkSize)
    val ss: Stream[F, Unit]   = listener.fold(src.through(tgt))(k => src.evalTap(k.run).through(tgt))

    sma.checkAndRun(ss)
  }
}

final class SaveMultiBinaryAvro[F[_], A](rdd: RDD[A], encoder: AvroEncoder[A], cfg: HoarderConfig)
    extends Serializable {
  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveMultiBinaryAvro[F, A] =
    new SaveMultiBinaryAvro[F, A](rdd, encoder, cfg)

  def append: SaveMultiBinaryAvro[F, A]         = updateConfig(cfg.appendMode)
  def overwrite: SaveMultiBinaryAvro[F, A]      = updateConfig(cfg.overwriteMode)
  def errorIfExists: SaveMultiBinaryAvro[F, A]  = updateConfig(cfg.errorMode)
  def ignoreIfExists: SaveMultiBinaryAvro[F, A] = updateConfig(cfg.ignoreMode)

  def run(implicit F: Sync[F]): F[Unit] =
    new SaveModeAware[F](params.saveMode, params.outPath, rdd.sparkContext.hadoopConfiguration)
      .checkAndRun(F.interruptibleMany(saveRDD.binAvro(rdd, params.outPath, encoder)))
}
