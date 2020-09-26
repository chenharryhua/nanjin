package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.{Blocker, Concurrent, ContextShift}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

final class SaveObjectFile[F[_], A](rdd: RDD[A], cfg: HoarderConfig) extends Serializable {

  val params: HoarderParams = cfg.evalConfig

  def run(
    blocker: Blocker)(implicit F: Concurrent[F], cs: ContextShift[F], tag: ClassTag[A]): F[Unit] =
    F.delay(rdd.saveAsObjectFile(params.outPath))
}
