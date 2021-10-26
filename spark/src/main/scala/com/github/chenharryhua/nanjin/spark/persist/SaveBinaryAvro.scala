package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.kernel.Sync
import com.github.chenharryhua.nanjin.spark.RddExt
import com.sksamuel.avro4s.Encoder as AvroEncoder
import fs2.{INothing, Stream}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD

final class SaveBinaryAvro[F[_], A](rdd: RDD[A], encoder: AvroEncoder[A], cfg: HoarderConfig) extends Serializable {
  def file: SaveSingleBinaryAvro[F, A]  = new SaveSingleBinaryAvro[F, A](rdd, encoder, cfg)
  def folder: SaveMultiBinaryAvro[F, A] = new SaveMultiBinaryAvro[F, A](rdd, encoder, cfg)
}

final class SaveSingleBinaryAvro[F[_], A](rdd: RDD[A], encoder: AvroEncoder[A], cfg: HoarderConfig)
    extends Serializable {
  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveBinaryAvro[F, A] =
    new SaveBinaryAvro[F, A](rdd, encoder, cfg)

  def overwrite: SaveBinaryAvro[F, A]      = updateConfig(cfg.overwriteMode)
  def errorIfExists: SaveBinaryAvro[F, A]  = updateConfig(cfg.errorMode)
  def ignoreIfExists: SaveBinaryAvro[F, A] = updateConfig(cfg.ignoreMode)

  def chunkSize(cs: Int): SaveBinaryAvro[F, A] = updateConfig(cfg.chunkSize(cs))

  def sink(implicit F: Sync[F]): Stream[F, INothing] = {
    val hc: Configuration     = rdd.sparkContext.hadoopConfiguration
    val sma: SaveModeAware[F] = new SaveModeAware[F](params.saveMode, params.outPath, hc)
    sma.checkAndRun(
      rdd.stream[F](params.chunkSize).through(sinks.binAvro(params.outPath, hc, encoder, params.chunkSize)))
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
      .checkAndRun(F.interruptible(many = true)(saveRDD.binAvro(rdd, params.outPath, encoder)))
}
