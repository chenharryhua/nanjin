package com.github.chenharryhua.nanjin.spark.persist

import cats.{Eq, Parallel}
import cats.effect.{Blocker, Concurrent, ContextShift}
import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.github.chenharryhua.nanjin.spark.{fileSink, RddExt}
import com.sksamuel.avro4s.Encoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

final class SaveBinaryAvro[F[_], A: ClassTag](rdd: RDD[A], codec: AvroCodec[A], cfg: HoarderConfig)(
  implicit ss: SparkSession)
    extends Serializable {
  val params: HoarderParams = cfg.evalConfig

  def run(blocker: Blocker)(implicit F: Concurrent[F], cs: ContextShift[F]): F[Unit] = {
    implicit val encoder: Encoder[A] = codec.avroEncoder

    val sma: SaveModeAware[F] = new SaveModeAware[F](params.saveMode, params.outPath, ss)

    sma.checkAndRun(blocker)(
      rdd.stream[F].through(fileSink[F](blocker).binAvro(params.outPath)).compile.drain)
  }
}

final class PartitionBinaryAvro[F[_], A: ClassTag, K: ClassTag: Eq](
  rdd: RDD[A],
  codec: AvroCodec[A],
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
      (r, p) => new SaveBinaryAvro[F, A](r, codec, cfg.withOutPutPath(p)).run(blocker))
}
