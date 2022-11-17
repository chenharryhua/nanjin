package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.kernel.Sync
import com.github.chenharryhua.nanjin.terminals.{JacksonCompression, NJCompression}
import com.sksamuel.avro4s.Encoder as AvroEncoder
import org.apache.spark.rdd.RDD

final class SaveJackson[F[_], A](frdd: F[RDD[A]], encoder: AvroEncoder[A], cfg: HoarderConfig)
    extends Serializable {

  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveJackson[F, A] =
    new SaveJackson[F, A](frdd, encoder, cfg)

  def append: SaveJackson[F, A]         = updateConfig(cfg.appendMode)
  def overwrite: SaveJackson[F, A]      = updateConfig(cfg.overwriteMode)
  def errorIfExists: SaveJackson[F, A]  = updateConfig(cfg.errorMode)
  def ignoreIfExists: SaveJackson[F, A] = updateConfig(cfg.ignoreMode)

  def bzip2: SaveJackson[F, A] = updateConfig(cfg.outputCompression(NJCompression.Bzip2))
  def deflate(level: Int): SaveJackson[F, A] = updateConfig(
    cfg.outputCompression(NJCompression.Deflate(level)))
  def gzip: SaveJackson[F, A]       = updateConfig(cfg.outputCompression(NJCompression.Gzip))
  def lz4: SaveJackson[F, A]        = updateConfig(cfg.outputCompression(NJCompression.Lz4))
  def uncompress: SaveJackson[F, A] = updateConfig(cfg.outputCompression(NJCompression.Uncompressed))
  def snappy: SaveJackson[F, A]     = updateConfig(cfg.outputCompression(NJCompression.Snappy))

  def withCompression(jc: JacksonCompression): SaveJackson[F, A] = updateConfig(cfg.outputCompression(jc))

  def run(implicit F: Sync[F]): F[Unit] =
    F.flatMap(frdd) { rdd =>
      new SaveModeAware[F](params.saveMode, params.outPath, rdd.sparkContext.hadoopConfiguration)
        .checkAndRun(F.interruptible(saveRDD.jackson(rdd, params.outPath, encoder, params.compression)))
    }
}
