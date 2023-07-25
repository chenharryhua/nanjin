package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.kernel.Sync
import com.github.chenharryhua.nanjin.terminals.{AvroCompression, NJCompression}
import com.sksamuel.avro4s.Encoder as AvroEncoder
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.Interval.Closed
import org.apache.spark.rdd.RDD

final class SaveAvro[F[_], A](frdd: F[RDD[A]], encoder: AvroEncoder[A], cfg: HoarderConfig)
    extends Serializable {

  private def updateConfig(cfg: HoarderConfig): SaveAvro[F, A] =
    new SaveAvro[F, A](frdd, encoder, cfg)

  val params: HoarderParams = cfg.evalConfig

  def append: SaveAvro[F, A]         = updateConfig(cfg.appendMode)
  def overwrite: SaveAvro[F, A]      = updateConfig(cfg.overwriteMode)
  def errorIfExists: SaveAvro[F, A]  = updateConfig(cfg.errorMode)
  def ignoreIfExists: SaveAvro[F, A] = updateConfig(cfg.ignoreMode)

  def bzip2: SaveAvro[F, A]               = updateConfig(cfg.outputCompression(NJCompression.Bzip2))
  def deflate(level: Int): SaveAvro[F, A] = updateConfig(cfg.outputCompression(NJCompression.Deflate(level)))
  def snappy: SaveAvro[F, A]              = updateConfig(cfg.outputCompression(NJCompression.Snappy))
  def uncompress: SaveAvro[F, A]          = updateConfig(cfg.outputCompression(NJCompression.Uncompressed))
  def xz(level: Int Refined Closed[1, 9]): SaveAvro[F, A] =
    updateConfig(cfg.outputCompression(NJCompression.Xz(level)))

  def withCompression(ac: AvroCompression): SaveAvro[F, A] = updateConfig(cfg.outputCompression(ac))

  def run(implicit F: Sync[F]): F[Unit] =
    F.flatMap(frdd) { rdd =>
      new SaveModeAware[F](params.saveMode, params.outPath, rdd.sparkContext.hadoopConfiguration)
        .checkAndRun(F.interruptible(saveRDD.avro(rdd, params.outPath, encoder, params.compression)))
    }
}
