package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.kernel.Sync
import com.github.chenharryhua.nanjin.terminals.AvroCompression
import com.sksamuel.avro4s.Encoder as AvroEncoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode

final class SaveAvro[A](rdd: RDD[A], encoder: AvroEncoder[A], cfg: HoarderConfig)
    extends Serializable with BuildRunnable {

  private def updateConfig(cfg: HoarderConfig): SaveAvro[A] =
    new SaveAvro[A](rdd, encoder, cfg)

  val params: HoarderParams = cfg.evalConfig

  def withSaveMode(sm: SaveMode): SaveAvro[A]                      = updateConfig(cfg.saveMode(sm))
  def withSaveMode(f: SparkSaveMode.type => SaveMode): SaveAvro[A] = withSaveMode(f(SparkSaveMode))

  def withCompression(ac: AvroCompression): SaveAvro[A] = updateConfig(cfg.outputCompression(ac))
  def withCompression(f: AvroCompression.type => AvroCompression): SaveAvro[A] =
    withCompression(f(AvroCompression))

  def run[F[_]](implicit F: Sync[F]): F[Unit] =
    new SaveModeAware[F](params.saveMode, params.outPath, rdd.sparkContext.hadoopConfiguration)
      .checkAndRun(F.interruptible(saveRDD.avro(rdd, params.outPath, encoder, params.compression)))

  def runWithCount[F[_]](implicit F: Sync[F]): F[Long] =
    F.map(run[F])(_ => rdd.count())
}
