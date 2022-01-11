package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.kernel.Async
import com.github.chenharryhua.nanjin.common.ChunkSize
import com.github.chenharryhua.nanjin.spark.RddExt
import fs2.Stream
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import scalapb.GeneratedMessage

final class SaveProtobuf[F[_], A](rdd: RDD[A], cfg: HoarderConfig) extends Serializable {
  def file: SaveSingleProtobuf[F, A]  = new SaveSingleProtobuf[F, A](rdd, cfg)
  def folder: SaveMultiProtobuf[F, A] = new SaveMultiProtobuf[F, A](rdd, cfg)
}

final class SaveSingleProtobuf[F[_], A](rdd: RDD[A], cfg: HoarderConfig) extends Serializable {

  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveSingleProtobuf[F, A] =
    new SaveSingleProtobuf[F, A](rdd, cfg)

  def overwrite: SaveSingleProtobuf[F, A]      = updateConfig(cfg.overwriteMode)
  def errorIfExists: SaveSingleProtobuf[F, A]  = updateConfig(cfg.errorMode)
  def ignoreIfExists: SaveSingleProtobuf[F, A] = updateConfig(cfg.ignoreMode)

  def withChunkSize(cs: ChunkSize): SaveSingleProtobuf[F, A] = updateConfig(cfg.chunkSize(cs))

  def sink(implicit F: Async[F], enc: A <:< GeneratedMessage): Stream[F, Unit] = {
    val hc: Configuration     = rdd.sparkContext.hadoopConfiguration
    val sma: SaveModeAware[F] = new SaveModeAware[F](params.saveMode, params.outPath, hc)
    sma.checkAndRun(rdd.stream[F](params.chunkSize).through(sinks.protobuf(params.outPath, hc, params.chunkSize)))
  }
}

final class SaveMultiProtobuf[F[_], A](rdd: RDD[A], cfg: HoarderConfig) extends Serializable {

  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveMultiProtobuf[F, A] =
    new SaveMultiProtobuf[F, A](rdd, cfg)

  def append: SaveMultiProtobuf[F, A]         = updateConfig(cfg.appendMode)
  def overwrite: SaveMultiProtobuf[F, A]      = updateConfig(cfg.overwriteMode)
  def errorIfExists: SaveMultiProtobuf[F, A]  = updateConfig(cfg.errorMode)
  def ignoreIfExists: SaveMultiProtobuf[F, A] = updateConfig(cfg.ignoreMode)

  def run(implicit F: Async[F], enc: A <:< GeneratedMessage): F[Unit] =
    new SaveModeAware[F](params.saveMode, params.outPath, rdd.sparkContext.hadoopConfiguration)
      .checkAndRun(F.interruptibleMany(saveRDD.protobuf(rdd, params.outPath)))
}
