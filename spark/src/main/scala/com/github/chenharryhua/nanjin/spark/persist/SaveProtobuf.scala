package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.{Blocker, Concurrent, ContextShift}
import cats.{Eq, Parallel}
import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.github.chenharryhua.nanjin.spark.RddExt
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scalapb.GeneratedMessage

import scala.reflect.ClassTag

final class SaveProtobuf[F[_], A: ClassTag](rdd: RDD[A], codec: AvroCodec[A], cfg: HoarderConfig)(
  implicit enc: A <:< GeneratedMessage) {

  val params: HoarderParams = cfg.evalConfig

  def run(
    blocker: Blocker)(implicit F: Concurrent[F], cs: ContextShift[F], ss: SparkSession): F[Unit] =
    rdd
      .map(codec.idConversion)
      .stream[F]
      .through(fileSink[F](blocker)(ss).protobuf[A](params.outPath))
      .compile
      .drain
}

final class PartitionProtobuf[F[_], A: ClassTag, K: ClassTag: Eq](
  rdd: RDD[A],
  codec: AvroCodec[A],
  cfg: HoarderConfig,
  bucketing: A => Option[K],
  pathBuilder: (NJFileFormat, K) => String)(implicit enc: A <:< GeneratedMessage)
    extends AbstractPartition[F, A, K] {

  val params: HoarderParams = cfg.evalConfig

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
      (r, p) => new SaveProtobuf[F, A](r, codec, cfg.withOutPutPath(p)).run(blocker))
}
