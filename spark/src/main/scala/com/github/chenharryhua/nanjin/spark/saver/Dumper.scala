package com.github.chenharryhua.nanjin.spark.saver

import cats.effect.{Blocker, ContextShift, Sync}
import com.github.chenharryhua.nanjin.spark.fileSink
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import cats.implicits._

final class Dumper[F[_], A](rdd: RDD[A], outPath: String) extends Serializable {

  def run(blocker: Blocker)(implicit F: Sync[F], cs: ContextShift[F], ss: SparkSession): F[Unit] =
    fileSink(blocker).delete(outPath).map { _ =>
      rdd.saveAsObjectFile(outPath)
    }
}
