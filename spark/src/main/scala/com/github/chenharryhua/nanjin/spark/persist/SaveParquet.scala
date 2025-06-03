package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.kernel.Sync
import com.github.chenharryhua.nanjin.terminals.ParquetCompression
import com.sksamuel.avro4s.Encoder as AvroEncoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode
final class SaveParquet[A](rdd: RDD[A], encoder: AvroEncoder[A], cfg: HoarderConfig)
    extends Serializable with BuildRunnable {

  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveParquet[A] =
    new SaveParquet[A](rdd, encoder, cfg)

  def withSaveMode(sm: SaveMode): SaveParquet[A] = updateConfig(cfg.saveMode(sm))
  def withSaveMode(f: SparkSaveMode.type => SaveMode): SaveParquet[A] = withSaveMode(f(SparkSaveMode))

  def withCompression(pc: ParquetCompression): SaveParquet[A] = updateConfig(cfg.outputCompression(pc))
  def withCompression(f: ParquetCompression.type => ParquetCompression): SaveParquet[A] =
    withCompression(f(ParquetCompression))

  def run[F[_]](implicit F: Sync[F]): F[Unit] =
    new SaveModeAware[F](params.saveMode, params.outPath, rdd.sparkContext.hadoopConfiguration)
      .checkAndRun(F.interruptible(saveRDD.parquet(rdd, params.outPath, encoder, params.compression)))

}
