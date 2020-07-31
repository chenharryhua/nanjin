package com.github.chenharryhua.nanjin.spark.saver

import cats.effect.{Blocker, Concurrent, ContextShift}
import com.github.chenharryhua.nanjin.spark.{fileSink, RddExt}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

final class JavaObjectSaver[A](rdd: RDD[A], outPath: String) extends Serializable {

  def run[F[_]](
    blocker: Blocker)(implicit F: Concurrent[F], ce: ContextShift[F], ss: SparkSession): F[Unit] =
    rdd.stream[F].through(fileSink[F](blocker).javaObject(outPath)).compile.drain

}
