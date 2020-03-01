package com.github.chenharryhua.nanjin

import cats.effect.{Async, ContextShift, Sync}
import com.github.chenharryhua.nanjin.pipes.{AkkaSingleFileSink, SingleFileSink, SingleFileSource}
import org.apache.spark.sql.SparkSession

package object spark extends DatasetExtensions {
  object injection extends InjectionInstances

  def fileSource[F[_]: Sync: ContextShift](implicit ss: SparkSession): SingleFileSource[F] =
    new SingleFileSource[F](ss.sparkContext.hadoopConfiguration)

  def fileSink[F[_]: Sync: ContextShift](implicit ss: SparkSession): SingleFileSink[F] =
    new SingleFileSink[F](ss.sparkContext.hadoopConfiguration)

  def akkaFileSink(implicit ss: SparkSession): AkkaSingleFileSink =
    new AkkaSingleFileSink(ss.sparkContext.hadoopConfiguration)

}
