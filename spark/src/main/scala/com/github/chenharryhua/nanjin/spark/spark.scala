package com.github.chenharryhua.nanjin

import akka.stream.Materializer
import akka.stream.alpakka.ftp.{FtpSettings, SftpSettings}
import cats.effect.{Concurrent, ConcurrentEffect, ContextShift, Sync}
import com.github.chenharryhua.nanjin.pipes._
import org.apache.spark.sql.SparkSession

package object spark extends DatasetExtensions {

  object injection extends InjectionInstances

  def fileSource[F[_]: Sync: ContextShift](implicit ss: SparkSession): SingleFileSource[F] =
    new SingleFileSource[F](ss.sparkContext.hadoopConfiguration)

  def fileSink[F[_]: ContextShift: Sync](implicit ss: SparkSession): SingleFileSink[F] =
    new SingleFileSink[F](ss.sparkContext.hadoopConfiguration)

  def ftpSink[F[_]: ConcurrentEffect: ContextShift](settings: FtpSettings)(implicit
    mat: Materializer): FtpSink[F] =
    new AkkaFtpSink[F](settings)

  def ftpSink[F[_]: ConcurrentEffect: ContextShift](settings: SftpSettings)(implicit
    mat: Materializer): FtpSink[F] =
    new AkkaSftpSink[F](settings)

  def ftpSource[F[_]: ContextShift: Concurrent](settings: FtpSettings)(implicit
    mat: Materializer): AkkaFtpSource[F] =
    new AkkaFtpSource[F](settings)

  def ftpSource[F[_]: ContextShift: Concurrent](settings: SftpSettings)(implicit
    mat: Materializer): AkkaSftpSource[F] =
    new AkkaSftpSource[F](settings)

}
