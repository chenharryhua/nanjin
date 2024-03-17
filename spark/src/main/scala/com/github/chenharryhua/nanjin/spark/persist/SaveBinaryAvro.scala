package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.kernel.Sync
import com.github.chenharryhua.nanjin.terminals.BinaryAvroCompression
import com.sksamuel.avro4s.Encoder as AvroEncoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode

final class SaveBinaryAvro[F[_], A](frdd: F[RDD[A]], encoder: AvroEncoder[A], cfg: HoarderConfig)
    extends Serializable with BuildRunnable[F] {

  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveBinaryAvro[F, A] =
    new SaveBinaryAvro[F, A](frdd, encoder, cfg)

  def withSaveMode(sm: SaveMode): SaveBinaryAvro[F, A]                   = updateConfig(cfg.saveMode(sm))
  def withSaveMode(f: NJSaveMode.type => SaveMode): SaveBinaryAvro[F, A] = withSaveMode(f(NJSaveMode))

  def withCompression(bc: BinaryAvroCompression): SaveBinaryAvro[F, A] =
    updateConfig(cfg.outputCompression(bc))
  def withCompression(f: BinaryAvroCompression.type => BinaryAvroCompression): SaveBinaryAvro[F, A] =
    withCompression(f(BinaryAvroCompression))

  def run(implicit F: Sync[F]): F[Unit] =
    F.flatMap(frdd) { rdd =>
      new SaveModeAware[F](params.saveMode, params.outPath, rdd.sparkContext.hadoopConfiguration)
        .checkAndRun(F.interruptible(saveRDD.binAvro(rdd, params.outPath, encoder, params.compression)))
    }
}
