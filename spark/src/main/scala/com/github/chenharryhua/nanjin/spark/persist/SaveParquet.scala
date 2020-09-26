package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.{Blocker, Concurrent, ContextShift}
import cats.{Eq, Parallel}
import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import com.sksamuel.avro4s.{Encoder => AvroEncoder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.reflect.ClassTag

final class SaveParquet[F[_], A](rdd: RDD[A], ate: AvroTypedEncoder[A], cfg: HoarderConfig)
    extends Serializable {

  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveParquet[F, A] =
    new SaveParquet[F, A](rdd, ate, cfg)

  def snappy: SaveParquet[F, A] = updateConfig(cfg.withCompression(Compression.Snappy))
  def gzip: SaveParquet[F, A]   = updateConfig(cfg.withCompression(Compression.Gzip))

  def run(
    blocker: Blocker)(implicit F: Concurrent[F], cs: ContextShift[F], ss: SparkSession): F[Unit] = {
    implicit val encoder: AvroEncoder[A] = ate.avroCodec.avroEncoder

    val sma: SaveModeAware[F] = new SaveModeAware[F](params.saveMode, params.outPath, ss)
    sma.checkAndRun(blocker)(
      F.delay(
        ate
          .normalize(rdd)
          .write
          .option("compression", params.compression.parquet.name)
          .mode(SaveMode.Overwrite)
          .parquet(params.outPath)))
  }
}
