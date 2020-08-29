package com.github.chenharryhua.nanjin.spark.saver

import cats.effect.{Blocker, ContextShift, Sync}
import com.github.chenharryhua.nanjin.spark.fileSink
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import cats.implicits._
import scala.reflect.ClassTag

final class ObjectFileSaver[F[_], A](rdd: RDD[A]) extends Serializable {

  def save(outPath: String, blocker: Blocker)(implicit
    F: Sync[F],
    cs: ContextShift[F],
    ss: SparkSession): F[Unit] =
    fileSink(blocker).delete(outPath).map { _ =>
      rdd.saveAsObjectFile(outPath)
    }
}

final class ObjectFileLoader[A: ClassTag](ss: SparkSession) extends Serializable {

  def load(pathStr: String): RDD[A] = ss.sparkContext.objectFile[A](pathStr)
}
