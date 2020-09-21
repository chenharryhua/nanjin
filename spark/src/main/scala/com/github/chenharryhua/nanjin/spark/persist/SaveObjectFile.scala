package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.{Blocker, Concurrent, ContextShift}
import cats.{Eq, Parallel}
import com.github.chenharryhua.nanjin.common.NJFileFormat
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

final class SaveObjectFile[F[_], A](rdd: RDD[A], cfg: HoarderConfig) extends Serializable {

  val params: HoarderParams = cfg.evalConfig

  def run(
    blocker: Blocker)(implicit F: Concurrent[F], cs: ContextShift[F], tag: ClassTag[A]): F[Unit] =
    F.delay(rdd.saveAsObjectFile(params.outPath))
}

final class PartitionObjectFile[F[_], A, K](
  rdd: RDD[A],
  cfg: HoarderConfig,
  bucketing: A => Option[K],
  pathBuilder: (NJFileFormat, K) => String)
    extends AbstractPartition[F, A, K] {

  val params: HoarderParams = cfg.evalConfig

  def run(blocker: Blocker)(implicit
    F: Concurrent[F],
    CS: ContextShift[F],
    P: Parallel[F],
    tagA: ClassTag[A],
    tagK: ClassTag[K],
    eq: Eq[K]): F[Unit] =
    savePartition(
      blocker,
      rdd,
      params.parallelism,
      params.format,
      bucketing,
      pathBuilder,
      (r, p) => new SaveObjectFile[F, A](r, cfg.withOutPutPath(p)).run(blocker))
}
