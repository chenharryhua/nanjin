package com.github.chenharryhua.nanjin.devices

import akka.stream.Materializer
import akka.stream.alpakka.ftp.scaladsl.{Ftp, FtpApi, Ftps, Sftp}
import akka.stream.alpakka.ftp.{FtpSettings, FtpsSettings, RemoteFileSettings, SftpSettings}
import cats.effect.{Async, Concurrent, ContextShift}
import cats.syntax.all._
import fs2.{Chunk, Stream}
import net.schmizz.sshj.SSHClient
import org.apache.commons.net.ftp.{FTPClient, FTPSClient}
import streamz.converter._

sealed abstract class FtpDownloader[F[_], C, S <: RemoteFileSettings](ftpApi: FtpApi[C, S], settings: S) {

  final def download(
    pathStr: String)(implicit F: Concurrent[F], cs: ContextShift[F], mat: Materializer): Stream[F, Byte] = {
    val run = ftpApi.fromPath(pathStr, settings).toStreamMat[F].map { case (s, f) =>
      s.concurrently(Stream.eval(Async.fromFuture(Async[F].pure(f))))
    }
    Stream.force(run).flatMap(bs => Stream.chunk(Chunk.bytes(bs.toArray)))
  }
}

final class AkkaFtpDownloader[F[_]](settings: FtpSettings)
    extends FtpDownloader[F, FTPClient, FtpSettings](Ftp, settings)

final class AkkaSftpDownloader[F[_]](settings: SftpSettings)
    extends FtpDownloader[F, SSHClient, SftpSettings](Sftp, settings)

final class AkkaFtpsDownloader[F[_]](settings: FtpsSettings)
    extends FtpDownloader[F, FTPSClient, FtpsSettings](Ftps, settings)
