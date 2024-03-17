package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.kernel.Sync
import com.github.chenharryhua.nanjin.terminals.JacksonCompression
import com.sksamuel.avro4s.Encoder as AvroEncoder
import org.apache.spark.rdd.RDD

final class SaveJackson[F[_], A](frdd: F[RDD[A]], encoder: AvroEncoder[A], cfg: HoarderConfig)
    extends Serializable with BuildRunnable[F] {

  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveJackson[F, A] =
    new SaveJackson[F, A](frdd, encoder, cfg)

  def append: SaveJackson[F, A]         = updateConfig(cfg.appendMode)
  def overwrite: SaveJackson[F, A]      = updateConfig(cfg.overwriteMode)
  def errorIfExists: SaveJackson[F, A]  = updateConfig(cfg.errorMode)
  def ignoreIfExists: SaveJackson[F, A] = updateConfig(cfg.ignoreMode)

  def withCompression(jc: JacksonCompression): SaveJackson[F, A] = updateConfig(cfg.outputCompression(jc))
  def withCompression(f: JacksonCompression.type => JacksonCompression): SaveJackson[F, A] =
    withCompression(f(JacksonCompression))

  def run(implicit F: Sync[F]): F[Unit] =
    F.flatMap(frdd) { rdd =>
      new SaveModeAware[F](params.saveMode, params.outPath, rdd.sparkContext.hadoopConfiguration)
        .checkAndRun(F.interruptible(saveRDD.jackson(rdd, params.outPath, encoder, params.compression)))
    }
}
