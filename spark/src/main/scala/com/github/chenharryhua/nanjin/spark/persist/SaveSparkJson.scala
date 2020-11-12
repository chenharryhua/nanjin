package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.{Blocker, Concurrent, ContextShift}
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

final class SaveSparkJson[F[_], A](
  rdd: RDD[A],
  ate: AvroTypedEncoder[A],
  cfg: HoarderConfig,
  isKeepNull: Boolean)
    extends Serializable {

  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveSparkJson[F, A] =
    new SaveSparkJson[F, A](rdd, ate, cfg, isKeepNull)

  def gzip: SaveSparkJson[F, A] =
    updateConfig(cfg.withCompression(Compression.Gzip))

  def deflate(level: Int): SaveSparkJson[F, A] =
    updateConfig(cfg.withCompression(Compression.Deflate(level)))

  def bzip2: SaveSparkJson[F, A] =
    updateConfig(cfg.withCompression(Compression.Bzip2))

  def keepNull: SaveSparkJson[F, A] = new SaveSparkJson[F, A](rdd, ate, cfg, true)
  def dropNull: SaveSparkJson[F, A] = new SaveSparkJson[F, A](rdd, ate, cfg, false)

  def run(
    blocker: Blocker)(implicit F: Concurrent[F], cs: ContextShift[F], ss: SparkSession): F[Unit] = {
    val sma: SaveModeAware[F] =
      new SaveModeAware[F](params.saveMode, params.outPath, ss)

    val ccg: CompressionCodecGroup[F] =
      params.compression.ccg[F](ss.sparkContext.hadoopConfiguration)

    sma.checkAndRun(blocker)(
      F.delay(
        ate
          .normalize(rdd)
          .write
          .mode(SaveMode.Overwrite)
          .option("compression", ccg.name)
          .option("ignoreNullFields", !isKeepNull)
          .json(params.outPath)))
  }
}
