package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.kernel.Sync
import com.github.chenharryhua.nanjin.terminals.BinaryAvroCompression
import com.sksamuel.avro4s.Encoder as AvroEncoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode

final class SaveBinaryAvro[A](rdd: RDD[A], encoder: AvroEncoder[A], cfg: HoarderConfig)
    extends Serializable with BuildRunnable {

  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveBinaryAvro[A] =
    new SaveBinaryAvro[A](rdd, encoder, cfg)

  def withSaveMode(sm: SaveMode): SaveBinaryAvro[A] = updateConfig(cfg.saveMode(sm))
  def withSaveMode(f: SparkSaveMode.type => SaveMode): SaveBinaryAvro[A] = withSaveMode(f(SparkSaveMode))

  def withCompression(bc: BinaryAvroCompression): SaveBinaryAvro[A] =
    updateConfig(cfg.outputCompression(bc))
  def withCompression(f: BinaryAvroCompression.type => BinaryAvroCompression): SaveBinaryAvro[A] =
    withCompression(f(BinaryAvroCompression))

  override def run[F[_]](description: String)(implicit F: Sync[F]): F[Unit] =
    internalRun(
      sparkContext = rdd.sparkContext,
      params = params,
      job = F.blocking(saveRDD.binAvro(rdd, params.outPath, encoder, params.compression)),
      description = Some(description)
    )

  override def run[F[_]](implicit F: Sync[F]): F[Unit] =
    internalRun(
      sparkContext = rdd.sparkContext,
      params = params,
      job = F.blocking(saveRDD.binAvro(rdd, params.outPath, encoder, params.compression)),
      description = None
    )
}
