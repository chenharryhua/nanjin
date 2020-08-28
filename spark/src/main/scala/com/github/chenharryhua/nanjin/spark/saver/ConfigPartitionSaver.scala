package com.github.chenharryhua.nanjin.spark.saver

import cats.Parallel
import cats.effect.implicits.catsSyntaxParallelTraverseNConcurrent
import cats.effect.{Blocker, Concurrent, ContextShift}
import cats.implicits._
import cats.kernel.Eq
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

final class ConfigPartitionSaver[F[_], A, K: ClassTag: Eq](
  rdd: RDD[A],
  cfs: ConfigFileSaver[F, A],
  parallelism: Long,
  bucketing: A => Option[K],
  pathBuilder: K => String) {

  def parallel(num: Long): ConfigPartitionSaver[F, A, K] =
    new ConfigPartitionSaver[F, A, K](rdd, cfs, num, bucketing, pathBuilder)

  def reBucket[K1: ClassTag: Eq](
    bucketing: A => Option[K1],
    pathBuilder: K1 => String): ConfigPartitionSaver[F, A, K1] =
    new ConfigPartitionSaver[F, A, K1](rdd, cfs, parallelism, bucketing, pathBuilder)

  def rePath(pathBuilder: K => String): ConfigPartitionSaver[F, A, K] =
    new ConfigPartitionSaver[F, A, K](rdd, cfs, parallelism, bucketing, pathBuilder)

  def run(blocker: Blocker)(implicit
    F: Concurrent[F],
    CS: ContextShift[F],
    P: Parallel[F],
    ss: SparkSession): F[Unit] =
    F.bracket(blocker.delay(rdd.persist())) { pr =>
      val keys: List[K] = pr.flatMap(bucketing(_)).distinct().collect().toList
      keys
        .parTraverseN(parallelism) { k =>
          cfs
            .withRDD(pr.filter(a => bucketing(a).exists(_ === k)))
            .withPath(pathBuilder(k))
            .run(blocker)
        }
        .void
    }(pr => blocker.delay(pr.unpersist()))
}
