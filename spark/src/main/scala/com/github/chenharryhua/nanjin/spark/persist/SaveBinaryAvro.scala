package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.kernel.Sync
import com.github.chenharryhua.nanjin.common.NJCompression
import com.sksamuel.avro4s.Encoder as AvroEncoder
import org.apache.spark.rdd.RDD

final class SaveBinaryAvro[F[_], A](rdd: RDD[A], encoder: AvroEncoder[A], cfg: HoarderConfig) extends Serializable {

  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveBinaryAvro[F, A] =
    new SaveBinaryAvro[F, A](rdd, encoder, cfg)

  def append: SaveBinaryAvro[F, A]         = updateConfig(cfg.appendMode)
  def overwrite: SaveBinaryAvro[F, A]      = updateConfig(cfg.overwriteMode)
  def errorIfExists: SaveBinaryAvro[F, A]  = updateConfig(cfg.errorMode)
  def ignoreIfExists: SaveBinaryAvro[F, A] = updateConfig(cfg.ignoreMode)

  def bzip2: SaveBinaryAvro[F, A]               = updateConfig(cfg.outputCompression(NJCompression.Bzip2))
  def deflate(level: Int): SaveBinaryAvro[F, A] = updateConfig(cfg.outputCompression(NJCompression.Deflate(level)))
  def gzip: SaveBinaryAvro[F, A]                = updateConfig(cfg.outputCompression(NJCompression.Gzip))
  def lz4: SaveBinaryAvro[F, A]                 = updateConfig(cfg.outputCompression(NJCompression.Lz4))
  def uncompress: SaveBinaryAvro[F, A]          = updateConfig(cfg.outputCompression(NJCompression.Uncompressed))

  def run(implicit F: Sync[F]): F[Unit] =
    new SaveModeAware[F](params.saveMode, params.outPath, rdd.sparkContext.hadoopConfiguration)
      .checkAndRun(F.interruptibleMany(saveRDD.binAvro(rdd, params.outPath, encoder, params.compression)))
}
