package com.github.chenharryhua.nanjin.spark

import akka.stream.alpakka.ftp.{FtpSettings, FtpsSettings, SftpSettings}
import cats.effect.Blocker
import com.github.chenharryhua.nanjin.terminals._
import net.schmizz.sshj.SSHClient
import org.apache.commons.net.ftp.{FTPClient, FTPSClient}

package object ftp {

  def ftpSink[F[_]](settings: FtpSettings, blocker: Blocker): FtpSink[F, FTPClient, FtpSettings] =
    new FtpSink(new AkkaFtpUploader[F](settings), blocker)

  def ftpSink[F[_]](settings: SftpSettings, blocker: Blocker): FtpSink[F, SSHClient, SftpSettings] =
    new FtpSink(new AkkaSftpUploader[F](settings), blocker)

  def ftpSink[F[_]](settings: FtpsSettings, blocker: Blocker): FtpSink[F, FTPSClient, FtpsSettings] =
    new FtpSink(new AkkaFtpsUploader[F](settings), blocker)

  def ftpSource[F[_]](settings: FtpSettings): FtpSource[F, FTPClient, FtpSettings] =
    new FtpSource(new AkkaFtpDownloader[F](settings))

  def ftpSource[F[_]](settings: SftpSettings): FtpSource[F, SSHClient, SftpSettings] =
    new FtpSource(new AkkaSftpDownloader[F](settings))

  def ftpSource[F[_]](settings: FtpsSettings): FtpSource[F, FTPSClient, FtpsSettings] =
    new FtpSource(new AkkaFtpsDownloader[F](settings))
}
