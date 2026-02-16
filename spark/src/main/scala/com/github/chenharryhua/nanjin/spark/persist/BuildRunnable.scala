package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.kernel.Sync
import cats.syntax.apply.catsSyntaxApplyOps
import cats.syntax.flatMap.catsSyntaxIfM
import com.github.chenharryhua.nanjin.spark.describeJob
import com.github.chenharryhua.nanjin.terminals.Hadoop
import org.apache.spark.SparkContext
import org.apache.spark.sql.SaveMode

trait BuildRunnable {
  final protected def internalRun[F[_]](
    sparkContext: SparkContext,
    params: HoarderParams,
    job: F[Unit],
    description: Option[String])(implicit F: Sync[F]): F[Unit] = {
    val jd: String = description.getOrElse(s"Save:${params.outPath.toString()}")
    describeJob[F](sparkContext, jd).surround {
      val hadoop = Hadoop[F](sparkContext.hadoopConfiguration)
      params.saveMode match {
        case SaveMode.Append        => job
        case SaveMode.Overwrite     => hadoop.delete(params.outPath) *> job
        case SaveMode.ErrorIfExists =>
          hadoop
            .isExist(params.outPath)
            .ifM(F.raiseError(new Exception(s"${params.outPath.toString()} already exist")), job)
        case SaveMode.Ignore =>
          hadoop.isExist(params.outPath).ifM(F.unit, job)
      }
    }
  }

  def run[F[_]](description: String)(implicit F: Sync[F]): F[Unit]
  def run[F[_]](implicit F: Sync[F]): F[Unit]
}
