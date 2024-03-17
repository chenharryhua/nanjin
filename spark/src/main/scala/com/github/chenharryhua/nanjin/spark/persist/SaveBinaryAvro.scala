package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.kernel.Sync
import com.github.chenharryhua.nanjin.terminals.{BinaryAvroCompression, NJCompression, NJCompressionLevel}
import com.sksamuel.avro4s.Encoder as AvroEncoder
import org.apache.spark.rdd.RDD

final class SaveBinaryAvro[F[_], A](frdd: F[RDD[A]], encoder: AvroEncoder[A], cfg: HoarderConfig)
    extends Serializable with BuildRunnable[F] {

  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveBinaryAvro[F, A] =
    new SaveBinaryAvro[F, A](frdd, encoder, cfg)

  def append: SaveBinaryAvro[F, A]         = updateConfig(cfg.appendMode)
  def overwrite: SaveBinaryAvro[F, A]      = updateConfig(cfg.overwriteMode)
  def errorIfExists: SaveBinaryAvro[F, A]  = updateConfig(cfg.errorMode)
  def ignoreIfExists: SaveBinaryAvro[F, A] = updateConfig(cfg.ignoreMode)

  def bzip2: SaveBinaryAvro[F, A] = updateConfig(cfg.outputCompression(NJCompression.Bzip2))
  def deflate(level: NJCompressionLevel): SaveBinaryAvro[F, A] =
    updateConfig(cfg.outputCompression(NJCompression.Deflate(level)))
  def gzip: SaveBinaryAvro[F, A]       = updateConfig(cfg.outputCompression(NJCompression.Gzip))
  def lz4: SaveBinaryAvro[F, A]        = updateConfig(cfg.outputCompression(NJCompression.Lz4))
  def snappy: SaveBinaryAvro[F, A]     = updateConfig(cfg.outputCompression(NJCompression.Snappy))
  def uncompressed: SaveBinaryAvro[F, A] = updateConfig(cfg.outputCompression(NJCompression.Uncompressed))

  def withCompression(bc: BinaryAvroCompression): SaveBinaryAvro[F, A] =
    updateConfig(cfg.outputCompression(bc))

  def run(implicit F: Sync[F]): F[Unit] =
    F.flatMap(frdd) { rdd =>
      new SaveModeAware[F](params.saveMode, params.outPath, rdd.sparkContext.hadoopConfiguration)
        .checkAndRun(F.interruptible(saveRDD.binAvro(rdd, params.outPath, encoder, params.compression)))
    }
}
