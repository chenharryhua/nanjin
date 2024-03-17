package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.kernel.Sync
import com.github.chenharryhua.nanjin.terminals.ParquetCompression
import com.sksamuel.avro4s.Encoder as AvroEncoder
import org.apache.spark.rdd.RDD
final class SaveParquet[F[_], A](frdd: F[RDD[A]], encoder: AvroEncoder[A], cfg: HoarderConfig)
    extends Serializable with BuildRunnable[F] {

  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveParquet[F, A] =
    new SaveParquet[F, A](frdd, encoder, cfg)

  def append: SaveParquet[F, A]         = updateConfig(cfg.appendMode)
  def overwrite: SaveParquet[F, A]      = updateConfig(cfg.overwriteMode)
  def errorIfExists: SaveParquet[F, A]  = updateConfig(cfg.errorMode)
  def ignoreIfExists: SaveParquet[F, A] = updateConfig(cfg.ignoreMode)

  def withCompression(pc: ParquetCompression): SaveParquet[F, A] = updateConfig(cfg.outputCompression(pc))
  def withCompression(f: ParquetCompression.type => ParquetCompression): SaveParquet[F, A] =
    withCompression(f(ParquetCompression))

  def run(implicit F: Sync[F]): F[Unit] =
    F.flatMap(frdd) { rdd =>
      new SaveModeAware[F](params.saveMode, params.outPath, rdd.sparkContext.hadoopConfiguration)
        .checkAndRun(F.interruptible(saveRDD.parquet(rdd, params.outPath, encoder, params.compression)))
    }
}
