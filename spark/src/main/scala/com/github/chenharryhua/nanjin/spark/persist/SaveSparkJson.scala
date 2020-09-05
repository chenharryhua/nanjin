package com.github.chenharryhua.nanjin.spark.persist

import cats.{Eq, Parallel}
import cats.effect.{Blocker, Concurrent, ContextShift}
import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.github.chenharryhua.nanjin.spark.utils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.reflect.ClassTag

final class SaveSparkJson[F[_], A: ClassTag](rdd: RDD[A], cfg: HoarderConfig)(implicit
  codec: AvroCodec[A],
  ss: SparkSession)
    extends Serializable {

  val params: HoarderParams = cfg.evalConfig

  def run(blocker: Blocker)(implicit F: Concurrent[F], cs: ContextShift[F]): F[Unit] = {
    val sma: SaveModeAware[F] = new SaveModeAware[F](params.saveMode, params.outPath, ss)

    sma.checkAndRun(blocker)(
      F.delay(
        utils
          .normalizedDF(rdd, codec.avroEncoder)
          .write
          .mode(SaveMode.Overwrite)
          .json(params.outPath)))
  }
}

final class PartitionSparkJson[F[_], A: ClassTag, K: ClassTag: Eq](
  rdd: RDD[A],
  cfg: HoarderConfig,
  bucketing: A => Option[K],
  pathBuilder: (NJFileFormat, K) => String)(implicit codec: AvroCodec[A], ss: SparkSession)
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
      (r, p) => new SaveSparkJson[F, A](r, cfg.withOutPutPath(p)).run(blocker))
}
