package com.github.chenharryhua.nanjin

import cats.effect.{ContextShift, Sync}
import com.github.chenharryhua.nanjin.pipes.{SingleFileSink, SingleFileSource}
import org.apache.spark.sql.SparkSession

package object spark extends DatasetExtensions {
  object injection extends InjectionInstances

  def fileSource[F[_]: Sync: ContextShift](implicit ss: SparkSession) =
    new SingleFileSource[F](ss.sparkContext.hadoopConfiguration)

  def fileSink[F[_]: Sync: ContextShift](implicit ss: SparkSession) =
    new SingleFileSink[F](ss.sparkContext.hadoopConfiguration)

}
