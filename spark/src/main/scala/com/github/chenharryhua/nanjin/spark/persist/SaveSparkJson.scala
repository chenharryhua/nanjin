package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.{Blocker, Concurrent, ContextShift}
import cats.{Eq, Parallel}
import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.reflect.ClassTag

final class SaveSparkJson[F[_], A](rdd: RDD[A], ate: AvroTypedEncoder[A], cfg: HoarderConfig)
    extends Serializable {

  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveSparkJson[F, A] =
    new SaveSparkJson[F, A](rdd, ate, cfg)

  def gzip: SaveSparkJson[F, A] = updateConfig(cfg.withCompression(Compression.Gzip))

  def deflate(level: Int): SaveSparkJson[F, A] = updateConfig(
    cfg.withCompression(Compression.Deflate(level)))

  def run(
    blocker: Blocker)(implicit F: Concurrent[F], cs: ContextShift[F], ss: SparkSession): F[Unit] = {
    val sma: SaveModeAware[F] = new SaveModeAware[F](params.saveMode, params.outPath, ss)

    sma.checkAndRun(blocker)(
      F.delay(
        ate
          .normalize(rdd)
          .write
          .mode(SaveMode.Overwrite)
          .option("compression", params.compression.ccg.name)
          .json(params.outPath)))
  }
}
