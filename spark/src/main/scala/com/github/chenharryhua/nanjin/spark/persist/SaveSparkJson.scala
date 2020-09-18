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

final class PartitionSparkJson[F[_], A: ClassTag, K: ClassTag: Eq](
  rdd: RDD[A],
  ate: AvroTypedEncoder[A],
  cfg: HoarderConfig,
  bucketing: A => Option[K],
  pathBuilder: (NJFileFormat, K) => String)
    extends AbstractPartition[F, A, K] {

  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): PartitionSparkJson[F, A, K] =
    new PartitionSparkJson[F, A, K](rdd, ate, cfg, bucketing, pathBuilder)

  def gzip: PartitionSparkJson[F, A, K] =
    updateConfig(cfg.withCompression(Compression.Gzip))

  def deflate(level: Int): PartitionSparkJson[F, A, K] =
    updateConfig(cfg.withCompression(Compression.Deflate(level)))

  def run(blocker: Blocker)(implicit
    F: Concurrent[F],
    CS: ContextShift[F],
    P: Parallel[F],
    ss: SparkSession): F[Unit] =
    savePartition(
      blocker,
      rdd,
      params.parallelism,
      params.format,
      bucketing,
      pathBuilder,
      (r, p) => new SaveSparkJson[F, A](r, ate, cfg.withOutPutPath(p)).run(blocker))
}
