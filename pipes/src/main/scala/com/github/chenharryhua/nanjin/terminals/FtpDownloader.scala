package com.github.chenharryhua.nanjin.terminals

import akka.stream.Materializer
import akka.stream.alpakka.ftp.scaladsl.{Ftp, FtpApi, Ftps, Sftp}
import akka.stream.alpakka.ftp.{FtpSettings, FtpsSettings, RemoteFileSettings, SftpSettings}
import akka.stream.scaladsl.Sink
import cats.effect.Async
import fs2.Stream
import fs2.interop.reactivestreams._
import net.schmizz.sshj.SSHClient
import org.apache.commons.net.ftp.{FTPClient, FTPSClient}

sealed abstract class FtpDownloader[F[_], C, S <: RemoteFileSettings](ftpApi: FtpApi[C, S], settings: S) {

  final def download(pathStr: String)(implicit F: Async[F], mat: Materializer): Stream[F, Byte] =
    Stream.suspend {
      for {
        bs <- ftpApi.fromPath(pathStr, settings).runWith(Sink.asPublisher(fanout = false)).toStream
        byte <- Stream.emits(bs)
      } yield byte
    }
}

object FtpDownloader {

  def apply[F[_]](settings: FtpSettings): FtpDownloader[F, FTPClient, FtpSettings] =
    new FtpDownloader[F, FTPClient, FtpSettings](Ftp, settings) {}

  def apply[F[_]](settings: SftpSettings): FtpDownloader[F, SSHClient, SftpSettings] =
    new FtpDownloader[F, SSHClient, SftpSettings](Sftp, settings) {}

  def apply[F[_]](settings: FtpsSettings): FtpDownloader[F, FTPSClient, FtpsSettings] =
    new FtpDownloader[F, FTPSClient, FtpsSettings](Ftps, settings) {}

}
