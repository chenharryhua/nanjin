package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.kernel.Sync
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode
import scalapb.GeneratedMessage

final class SaveProtobuf[A](rdd: RDD[A], cfg: HoarderConfig, evidence: A <:< GeneratedMessage)
    extends Serializable with BuildRunnable {

  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveProtobuf[A] =
    new SaveProtobuf[A](rdd, cfg, evidence)

  def withSaveMode(sm: SaveMode): SaveProtobuf[A]                      = updateConfig(cfg.saveMode(sm))
  def withSaveMode(f: SparkSaveMode.type => SaveMode): SaveProtobuf[A] = withSaveMode(f(SparkSaveMode))

  def run[F[_]](implicit F: Sync[F]): F[Unit] =
    new SaveModeAware[F](params.saveMode, params.outPath, rdd.sparkContext.hadoopConfiguration)
      .checkAndRun(F.interruptible(saveRDD.protobuf(rdd, params.outPath, params.compression)(evidence)))

}
