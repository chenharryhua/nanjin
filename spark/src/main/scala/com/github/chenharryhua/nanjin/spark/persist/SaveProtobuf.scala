package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.kernel.Sync
import org.apache.spark.rdd.RDD
import scalapb.GeneratedMessage

final class SaveProtobuf[F[_], A](frdd: F[RDD[A]], cfg: HoarderConfig, evidence: A <:< GeneratedMessage)
    extends Serializable with BuildRunnable[F] {

  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveProtobuf[F, A] =
    new SaveProtobuf[F, A](frdd, cfg, evidence)

  def append: SaveProtobuf[F, A]         = updateConfig(cfg.appendMode)
  def overwrite: SaveProtobuf[F, A]      = updateConfig(cfg.overwriteMode)
  def errorIfExists: SaveProtobuf[F, A]  = updateConfig(cfg.errorMode)
  def ignoreIfExists: SaveProtobuf[F, A] = updateConfig(cfg.ignoreMode)

  def run(implicit F: Sync[F]): F[Unit] =
    F.flatMap(frdd) { rdd =>
      new SaveModeAware[F](params.saveMode, params.outPath, rdd.sparkContext.hadoopConfiguration)
        .checkAndRun(F.interruptible(saveRDD.protobuf(rdd, params.outPath, params.compression)(evidence)))
    }
}
