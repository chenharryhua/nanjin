package com.github.chenharryhua.nanjin.devices

import akka.stream.Materializer
import akka.stream.alpakka.ftp.scaladsl.{Ftp, FtpApi, Ftps, Sftp}
import akka.stream.alpakka.ftp.{FtpSettings, FtpsSettings, RemoteFileSettings, SftpSettings}
import cats.effect.{Async, Concurrent, ContextShift}
import cats.implicits._
import fs2.{Chunk, Stream}
import net.schmizz.sshj.SSHClient
import org.apache.commons.net.ftp.{FTPClient, FTPSClient}
import streamz.converter._

sealed class FtpDownloader[F[_]: ContextShift: Concurrent, C, S <: RemoteFileSettings](
  ftpApi: FtpApi[C, S],
  settings: S)(implicit mat: Materializer) {

  final def download(pathStr: String): Stream[F, Byte] = {
    val run = ftpApi.fromPath(pathStr, settings).toStreamMat[F].map {
      case (s, f) =>
        s.concurrently(Stream.eval(Async.fromFuture(Async[F].pure(f))))
    }
    Stream.force(run).flatMap(bs => Stream.chunk(Chunk.bytes(bs.toArray)))
  }
}

final class AkkaFtpDownloader[F[_]: ContextShift: Concurrent](settings: FtpSettings)(implicit
  mat: Materializer)
    extends FtpDownloader[F, FTPClient, FtpSettings](Ftp, settings)

final class AkkaSftpDownloader[F[_]: ContextShift: Concurrent](settings: SftpSettings)(implicit
  mat: Materializer)
    extends FtpDownloader[F, SSHClient, SftpSettings](Sftp, settings)

final class AkkaFtpsDownloader[F[_]: ContextShift: Concurrent](settings: FtpsSettings)(implicit
  mat: Materializer)
    extends FtpDownloader[F, FTPSClient, FtpsSettings](Ftps, settings)
