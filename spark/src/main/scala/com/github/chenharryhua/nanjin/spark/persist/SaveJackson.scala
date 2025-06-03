package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.kernel.Sync
import com.github.chenharryhua.nanjin.terminals.JacksonCompression
import com.sksamuel.avro4s.Encoder as AvroEncoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode

final class SaveJackson[A](rdd: RDD[A], encoder: AvroEncoder[A], cfg: HoarderConfig)
    extends Serializable with BuildRunnable {

  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveJackson[A] =
    new SaveJackson[A](rdd, encoder, cfg)

  def withSaveMode(sm: SaveMode): SaveJackson[A] = updateConfig(cfg.saveMode(sm))
  def withSaveMode(f: SparkSaveMode.type => SaveMode): SaveJackson[A] = withSaveMode(f(SparkSaveMode))

  def withCompression(jc: JacksonCompression): SaveJackson[A] = updateConfig(cfg.outputCompression(jc))
  def withCompression(f: JacksonCompression.type => JacksonCompression): SaveJackson[A] =
    withCompression(f(JacksonCompression))

  def run[F[_]](implicit F: Sync[F]): F[Unit] =
    new SaveModeAware[F](params.saveMode, params.outPath, rdd.sparkContext.hadoopConfiguration)
      .checkAndRun(F.interruptible(saveRDD.jackson(rdd, params.outPath, encoder, params.compression)))
}
