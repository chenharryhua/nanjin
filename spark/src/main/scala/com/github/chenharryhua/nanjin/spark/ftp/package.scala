package com.github.chenharryhua.nanjin.spark

import akka.stream.Materializer
import akka.stream.alpakka.ftp.{FtpSettings, FtpsSettings, SftpSettings}
import cats.effect.{Blocker, Concurrent, ConcurrentEffect, ContextShift}
import com.github.chenharryhua.nanjin.devices.{
  AkkaFtpDownloader,
  AkkaFtpUploader,
  AkkaFtpsDownloader,
  AkkaFtpsUploader,
  AkkaSftpDownloader,
  AkkaSftpUploader
}
import net.schmizz.sshj.SSHClient
import org.apache.commons.net.ftp.{FTPClient, FTPSClient}

package object ftp {

  def ftpSink[F[_]: ConcurrentEffect: ContextShift](settings: FtpSettings, blocker: Blocker)(
    implicit mat: Materializer): FtpSink[F, FTPClient, FtpSettings] =
    new FtpSink(new AkkaFtpUploader[F](settings), blocker)

  def ftpSink[F[_]: ConcurrentEffect: ContextShift](settings: SftpSettings, blocker: Blocker)(
    implicit mat: Materializer): FtpSink[F, SSHClient, SftpSettings] =
    new FtpSink(new AkkaSftpUploader[F](settings), blocker)

  def ftpSink[F[_]: ConcurrentEffect: ContextShift](settings: FtpsSettings, blocker: Blocker)(
    implicit mat: Materializer): FtpSink[F, FTPSClient, FtpsSettings] =
    new FtpSink(new AkkaFtpsUploader[F](settings), blocker)

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
