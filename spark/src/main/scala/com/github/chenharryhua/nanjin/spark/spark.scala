package com.github.chenharryhua.nanjin

import akka.stream.alpakka.ftp.FtpSettings
import cats.effect.{Async, ConcurrentEffect, ContextShift, Sync}
import com.github.chenharryhua.nanjin.pipes._
import org.apache.spark.sql.SparkSession

package object spark extends DatasetExtensions {

  object injection extends InjectionInstances

  def fileSource[F[_]: Sync: ContextShift](implicit ss: SparkSession): SingleFileSource[F] =
    new SingleFileSource[F](ss.sparkContext.hadoopConfiguration)

  def fileSink[F[_]: ContextShift: Sync](implicit ss: SparkSession): SingleFileSink[F] =
    new SingleFileSink[F](ss.sparkContext.hadoopConfiguration)

  def akkaFileSource(implicit ss: SparkSession): AkkaSingleFileSource =
    new AkkaSingleFileSource(ss.sparkContext.hadoopConfiguration)

  def akkaFileSink[F[_]: ConcurrentEffect](implicit ss: SparkSession): AkkaSingleFileSink[F] =
    new AkkaSingleFileSink(ss.sparkContext.hadoopConfiguration)

  def akkaFtpSink[F[_]: Async: ContextShift](settings: FtpSettings): AkkaFtpSink[F] =
    new AkkaFtpSink[F](settings)
}
