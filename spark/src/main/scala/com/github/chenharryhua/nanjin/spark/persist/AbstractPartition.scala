package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.implicits.catsSyntaxParallelTraverseNConcurrent
import cats.effect.{Blocker, Concurrent, ContextShift}
import cats.syntax.all._
import cats.{Eq, Parallel}
import com.github.chenharryhua.nanjin.common.NJFileFormat
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

abstract class AbstractPartition[F[_], A, K] extends Serializable {

  protected def savePartition(
    blocker: Blocker,
    rdd: RDD[A],
    parallelism: Long,
    fmt: NJFileFormat,
    bucketing: A => Option[K],
    pathBuilder: (NJFileFormat, K) => String,
    save: (RDD[A], String) => F[Unit]
  )(implicit
    F: Concurrent[F],
    CS: ContextShift[F],
    P: Parallel[F],
    kTag: ClassTag[K],
    eqK: Eq[K]): F[Unit] =
    F.bracket(blocker.delay(rdd.persist())) { pr =>
      val keys: List[K] = pr.flatMap(bucketing(_)).distinct().collect().toList
      keys
        .parTraverseN(parallelism) { k =>
          val pRDD: RDD[A] = pr.filter(a => bucketing(a).exists(_ === k))
          val path: String = pathBuilder(fmt, k)
          save(pRDD, path)
        }
        .void
    }(pr => blocker.delay(pr.unpersist()))
}
