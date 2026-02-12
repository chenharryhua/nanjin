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

  def withSaveMode(sm: SaveMode): SaveProtobuf[A] = updateConfig(cfg.saveMode(sm))
  def withSaveMode(f: SparkSaveMode.type => SaveMode): SaveProtobuf[A] = withSaveMode(f(SparkSaveMode))

  def run[F[_]](implicit F: Sync[F]): F[Unit] =
    internalRun(
      sparkContext = rdd.sparkContext,
      params = params,
      job = F.blocking(saveRDD.protobuf(rdd, params.outPath, params.compression)(evidence)),
      description = None
    )

  override def run[F[_]](description: String)(implicit F: Sync[F]): F[Unit] =
    internalRun(
      sparkContext = rdd.sparkContext,
      params = params,
      job = F.blocking(saveRDD.protobuf(rdd, params.outPath, params.compression)(evidence)),
      description = Some(description)
    )

}
