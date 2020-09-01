package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.{Blocker, Concurrent, ContextShift}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

final class SaveObjectFile[F[_], A: ClassTag](rdd: RDD[A], cfg: HoarderConfig)
    extends Serializable {

  val params: HoarderParams = cfg.evalConfig

  def run(blocker: Blocker)(implicit F: Concurrent[F], cs: ContextShift[F]): F[Unit] =
    F.delay(rdd.saveAsObjectFile(params.outPath))
}
