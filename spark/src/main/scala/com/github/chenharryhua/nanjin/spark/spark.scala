package com.github.chenharryhua.nanjin

import akka.stream.Materializer
import akka.stream.alpakka.ftp.{FtpSettings, FtpsSettings, SftpSettings}
import cats.effect.{Blocker, Concurrent, ConcurrentEffect, ContextShift}
import com.github.chenharryhua.nanjin.devices._
import net.schmizz.sshj.SSHClient
import org.apache.commons.net.ftp.{FTPClient, FTPSClient}
import org.apache.spark.sql.SparkSession

package object spark extends DatasetExtensions {

  object injection extends InjectionInstances

  def fileSink[F[_]](blocker: Blocker)(implicit ss: SparkSession): SingleFileSink[F] =
    new SingleFileSink[F](blocker, ss.sparkContext.hadoopConfiguration)

  def fileSource[F[_]](blocker: Blocker)(implicit ss: SparkSession): SingleFileSource[F] =
    new SingleFileSource[F](blocker, ss.sparkContext.hadoopConfiguration)

  def ftpSink[F[_]: ConcurrentEffect: ContextShift](settings: FtpSettings)(implicit
    mat: Materializer): FtpSink[F, FTPClient, FtpSettings] =
    new FtpSink(new AkkaFtpUploader[F](settings))

  def ftpSink[F[_]: ConcurrentEffect: ContextShift](settings: SftpSettings)(implicit
    mat: Materializer): FtpSink[F, SSHClient, SftpSettings] =
    new FtpSink(new AkkaSftpUploader[F](settings))

  def ftpSink[F[_]: ConcurrentEffect: ContextShift](settings: FtpsSettings)(implicit
    mat: Materializer): FtpSink[F, FTPSClient, FtpsSettings] =
    new FtpSink(new AkkaFtpsUploader[F](settings))

  def ftpSource[F[_]: ContextShift: Concurrent](settings: FtpSettings)(implicit
    mat: Materializer): FtpSource[F, FTPClient, FtpSettings] =
    new FtpSource(new AkkaFtpDownloader[F](settings))

  def ftpSource[F[_]: ContextShift: Concurrent](settings: SftpSettings)(implicit
    mat: Materializer): FtpSource[F, SSHClient, SftpSettings] =
    new FtpSource(new AkkaSftpDownloader[F](settings))

  def ftpSource[F[_]: ContextShift: Concurrent](settings: FtpsSettings)(implicit
    mat: Materializer): FtpSource[F, FTPSClient, FtpsSettings] =
    new FtpSource(new AkkaFtpsDownloader[F](settings))
}
