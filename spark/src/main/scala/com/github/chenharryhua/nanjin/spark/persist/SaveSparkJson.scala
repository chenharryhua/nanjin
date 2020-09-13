package com.github.chenharryhua.nanjin.spark.persist

import cats.{Eq, Parallel}
import cats.effect.{Blocker, Concurrent, ContextShift}
import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.reflect.ClassTag

final class SaveSparkJson[F[_], A](rdd: RDD[A], ate: AvroTypedEncoder[A], cfg: HoarderConfig)(
  implicit ss: SparkSession)
    extends Serializable {

  val params: HoarderParams = cfg.evalConfig

  def run(blocker: Blocker)(implicit F: Concurrent[F], cs: ContextShift[F]): F[Unit] = {
    val sma: SaveModeAware[F] = new SaveModeAware[F](params.saveMode, params.outPath, ss)

    sma.checkAndRun(blocker)(
      F.delay(ate.normalize(rdd).write.mode(SaveMode.Overwrite).json(params.outPath)))
  }
}

final class PartitionSparkJson[F[_], A: ClassTag, K: ClassTag: Eq](
  rdd: RDD[A],
  ate: AvroTypedEncoder[A],
  cfg: HoarderConfig,
  bucketing: A => Option[K],
  pathBuilder: (NJFileFormat, K) => String)(implicit ss: SparkSession)
    extends AbstractPartition[F, A, K] {

  val params: HoarderParams = cfg.evalConfig

  def run(
    blocker: Blocker)(implicit F: Concurrent[F], CS: ContextShift[F], P: Parallel[F]): F[Unit] =
    savePartition(
      blocker,
      rdd,
      params.parallelism,
      params.format,
      bucketing,
      pathBuilder,
      (r, p) => new SaveSparkJson[F, A](r, ate, cfg.withOutPutPath(p)).run(blocker))
}
