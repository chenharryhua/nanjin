package com.github.chenharryhua.nanjin

import cats.effect.{Async, ContextShift, Sync}
import com.github.chenharryhua.nanjin.pipes.{
  AkkaSingleFileSink,
  AkkaSingleFileSource,
  SingleFileSink,
  SingleFileSource
}
import org.apache.spark.sql.SparkSession
import cats.effect.ConcurrentEffect

package object spark extends DatasetExtensions {

  object injection extends InjectionInstances

  def fileSource[F[_]: Sync: ContextShift](implicit ss: SparkSession): SingleFileSource[F] =
    new SingleFileSource[F](ss.sparkContext.hadoopConfiguration)

  def fileSink[F[_]: Sync: ContextShift](implicit ss: SparkSession): SingleFileSink[F] =
    new SingleFileSink[F](ss.sparkContext.hadoopConfiguration)

  def akkaFileSink[F[_]: ConcurrentEffect](implicit ss: SparkSession): AkkaSingleFileSink[F] =
    new AkkaSingleFileSink[F](ss.sparkContext.hadoopConfiguration)

  def akkaFileSource(implicit ss: SparkSession): AkkaSingleFileSource =
    new AkkaSingleFileSource(ss.sparkContext.hadoopConfiguration)

}
