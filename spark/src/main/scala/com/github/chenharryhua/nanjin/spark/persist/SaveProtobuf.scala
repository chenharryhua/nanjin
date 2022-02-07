package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.kernel.Sync
import org.apache.spark.rdd.RDD
import scalapb.GeneratedMessage

final class SaveProtobuf[F[_], A](rdd: RDD[A], cfg: HoarderConfig) extends Serializable {

  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveProtobuf[F, A] =
    new SaveProtobuf[F, A](rdd, cfg)

  def append: SaveProtobuf[F, A]         = updateConfig(cfg.appendMode)
  def overwrite: SaveProtobuf[F, A]      = updateConfig(cfg.overwriteMode)
  def errorIfExists: SaveProtobuf[F, A]  = updateConfig(cfg.errorMode)
  def ignoreIfExists: SaveProtobuf[F, A] = updateConfig(cfg.ignoreMode)

  def run(implicit F: Sync[F], enc: A <:< GeneratedMessage): F[Unit] =
    new SaveModeAware[F](params.saveMode, params.outPath, rdd.sparkContext.hadoopConfiguration)
      .checkAndRun(F.interruptibleMany(saveRDD.protobuf(rdd, params.outPath)))
}
