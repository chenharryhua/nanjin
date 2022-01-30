package com.github.chenharryhua.nanjin.terminals

import akka.stream.alpakka.ftp.scaladsl.{Ftp, FtpApi, Ftps, Sftp}
import akka.stream.alpakka.ftp.{FtpSettings, FtpsSettings, RemoteFileSettings, SftpSettings}
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{IOResult, Materializer}
import akka.util.ByteString
import cats.effect.kernel.Async
import com.github.chenharryhua.nanjin.common.ChunkSize
import fs2.interop.reactivestreams.StreamOps
import fs2.{Pipe, Stream}
import net.schmizz.sshj.SSHClient
import org.apache.commons.net.ftp.{FTPClient, FTPSClient}

import scala.concurrent.Future

sealed abstract class FtpUploader[F[_], C, S <: RemoteFileSettings](ftpApi: FtpApi[C, S], settings: S) {

  final def upload(pathStr: String)(implicit F: Async[F], mat: Materializer): Pipe[F, Byte, IOResult] = {
    val sink: Sink[ByteString, Future[IOResult]] = ftpApi.toPath(pathStr, settings)
    (ss: Stream[F, Byte]) =>
      Stream.eval(ss.chunks.toUnicastPublisher.use { p =>
        F.fromFuture(F.blocking(Source.fromPublisher(p).map(x => ByteString.apply(x.toArray)).runWith(sink)))
      })
  }
}

object FtpUploader {

  def apply[F[_]](settings: FtpSettings): FtpUploader[F, FTPClient, FtpSettings] =
    new FtpUploader[F, FTPClient, FtpSettings](Ftp, settings) {}

  def apply[F[_]](settings: SftpSettings): FtpUploader[F, SSHClient, SftpSettings] =
    new FtpUploader[F, SSHClient, SftpSettings](Sftp, settings) {}

  def apply[F[_]](settings: FtpsSettings): FtpUploader[F, FTPSClient, FtpsSettings] =
    new FtpUploader[F, FTPSClient, FtpsSettings](Ftps, settings) {}
}
