package com.github.chenharryhua.nanjin.devices

import akka.stream.alpakka.ftp.scaladsl.{Ftp, FtpApi, Ftps, Sftp}
import akka.stream.alpakka.ftp.{FtpSettings, FtpsSettings, RemoteFileSettings, SftpSettings}
import akka.stream.{IOResult, Materializer}
import akka.util.ByteString
import cats.effect.{ConcurrentEffect, ContextShift}
import fs2.{Pipe, Stream}
import net.schmizz.sshj.SSHClient
import org.apache.commons.net.ftp.{FTPClient, FTPSClient}
import streamz.converter._

sealed class FtpUploader[F[_]: ConcurrentEffect: ContextShift, C, S <: RemoteFileSettings](
  ftpApi: FtpApi[C, S],
  settings: S)(implicit mat: Materializer) {

  import mat.executionContext

  final def upload(pathStr: String): Pipe[F, Byte, IOResult] = { (ss: Stream[F, Byte]) =>
    Stream
      .eval(ftpApi.toPath(pathStr, settings).toPipeMatWithResult[F])
      .flatMap(p => ss.chunks.through(p.compose(_.map(bs => ByteString(bs.toArray)))).rethrow)
  }
}

final class AkkaFtpUploader[F[_]: ConcurrentEffect: ContextShift](settings: FtpSettings)(implicit
  mat: Materializer)
    extends FtpUploader[F, FTPClient, FtpSettings](Ftp, settings)

final class AkkaSftpUploader[F[_]: ConcurrentEffect: ContextShift](settings: SftpSettings)(implicit
  mat: Materializer)
    extends FtpUploader[F, SSHClient, SftpSettings](Sftp, settings)

final class AkkaFtpsUploader[F[_]: ContextShift: ConcurrentEffect](settings: FtpsSettings)(implicit
  mat: Materializer)
    extends FtpUploader[F, FTPSClient, FtpsSettings](Ftps, settings)
