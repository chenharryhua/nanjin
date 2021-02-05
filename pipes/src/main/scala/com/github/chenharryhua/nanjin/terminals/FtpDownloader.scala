package com.github.chenharryhua.nanjin.terminals

import akka.stream.Materializer
import akka.stream.alpakka.ftp.scaladsl.{Ftp, FtpApi, Ftps, Sftp}
import akka.stream.alpakka.ftp.{FtpSettings, FtpsSettings, RemoteFileSettings, SftpSettings}
import akka.stream.scaladsl.Sink
import cats.effect.{ConcurrentEffect, ContextShift}
import fs2.Stream
import fs2.interop.reactivestreams._
import net.schmizz.sshj.SSHClient
import org.apache.commons.net.ftp.{FTPClient, FTPSClient}

sealed abstract class FtpDownloader[F[_], C, S <: RemoteFileSettings](ftpApi: FtpApi[C, S], settings: S) {

  final def download(
    pathStr: String)(implicit F: ConcurrentEffect[F], cs: ContextShift[F], mat: Materializer): Stream[F, Byte] =
    Stream.suspend {
      for {
        bs <- ftpApi.fromPath(pathStr, settings).runWith(Sink.asPublisher(fanout = false)).toStream
        byte <- Stream.emits(bs)
      } yield byte
    }
}

final class AkkaFtpDownloader[F[_]](settings: FtpSettings)
    extends FtpDownloader[F, FTPClient, FtpSettings](Ftp, settings)

final class AkkaSftpDownloader[F[_]](settings: SftpSettings)
    extends FtpDownloader[F, SSHClient, SftpSettings](Sftp, settings)

final class AkkaFtpsDownloader[F[_]](settings: FtpsSettings)
    extends FtpDownloader[F, FTPSClient, FtpsSettings](Ftps, settings)
