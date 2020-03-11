package com.github.chenharryhua.nanjin

import cats.effect.{ContextShift, Sync}
import com.github.chenharryhua.nanjin.pipes.{
  AkkaSingleFileSink,
  AkkaSingleFileSource,
  SingleFileSink,
  SingleFileSource
}
import org.apache.spark.sql.SparkSession

package object spark extends DatasetExtensions {

  object injection extends InjectionInstances

  def fileSource[F[_]: Sync: ContextShift](implicit ss: SparkSession): SingleFileSource[F] =
    new SingleFileSource[F](ss.sparkContext.hadoopConfiguration)

  def akkaFileSource(implicit ss: SparkSession): AkkaSingleFileSource =
    new AkkaSingleFileSource(ss.sparkContext.hadoopConfiguration)

  def fileSink(implicit ss: SparkSession): SingleFileSink =
    new SingleFileSink(ss.sparkContext.hadoopConfiguration)

  def akkaFileSink(implicit ss: SparkSession): AkkaSingleFileSink =
    new AkkaSingleFileSink(ss.sparkContext.hadoopConfiguration)

}
